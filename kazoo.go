package kazoo

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"math/rand"
	"regexp"
	"sort"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	maxTopicNameLength = 255
	deleteTopicsPath   = "admin/delete_topics"
	brokerTopicsPath   = "brokers/topics"
	configChangePath   = "/config/changes/config_change_"
	configRootPath     = "config"
)

var (
	FailedToClaimPartition = errors.New("Failed to claim partition for this consumer instance. Do you have a rogue consumer running?")
)

// ParseConnectionString parses a zookeeper connection string in the form of
// host1:2181,host2:2181/chroot and returns the list of servers, and the chroot.
func ParseConnectionString(zookeeper string) (nodes []string, chroot string) {
	nodesAndChroot := strings.SplitN(zookeeper, "/", 2)
	if len(nodesAndChroot) == 2 {
		chroot = fmt.Sprintf("/%s", nodesAndChroot[1])
	}
	nodes = strings.Split(nodesAndChroot[0], ",")
	return
}

// BuildConnectionString builds a Zookeeper connection string for a list of nodes.
// Returns a string like "zk1:2181,zk2:2181,zk3:2181"
func BuildConnectionString(nodes []string) string {
	return strings.Join(nodes, ",")
}

// ConnectionStringWithChroot builds a Zookeeper connection string for a list
// of nodes and a chroot. The chroot should start with "/".
// Returns a string like "zk1:2181,zk2:2181,zk3:2181/chroot"
func BuildConnectionStringWithChroot(nodes []string, chroot string) string {
	return fmt.Sprintf("%s%s", strings.Join(nodes, ","), chroot)
}

// Kazoo interacts with the Kafka metadata in Zookeeper
type Kazoo struct {
	conn *zk.Conn
	conf *Config
}

// Config holds configuration values f.
type Config struct {
	// The chroot the Kafka installation is registerde under. Defaults to "".
	Chroot string

	// The amount of time the Zookeeper client can be disconnected from the Zookeeper cluster
	// before the cluster will get rid of watches and ephemeral nodes. Defaults to 1 second.
	Timeout time.Duration
}

// NewConfig instantiates a new Config struct with sane defaults.
func NewConfig() *Config {
	return &Config{Timeout: 1 * time.Second}
}

// NewKazoo creates a new connection instance
func NewKazoo(servers []string, conf *Config) (*Kazoo, error) {
	if conf == nil {
		conf = NewConfig()
	}

	conn, _, err := zk.Connect(servers, conf.Timeout)
	if err != nil {
		return nil, err
	}
	return &Kazoo{conn, conf}, nil
}

// NewKazooFromConnectionString creates a new connection instance
// based on a zookeeer connection string that can include a chroot.
func NewKazooFromConnectionString(connectionString string, conf *Config) (*Kazoo, error) {
	if conf == nil {
		conf = NewConfig()
	}

	nodes, chroot := ParseConnectionString(connectionString)
	conf.Chroot = chroot
	return NewKazoo(nodes, conf)
}

// Brokers returns a map of all the brokers that make part of the
// Kafka cluster that is regeistered in Zookeeper.
func (kz *Kazoo) Brokers() (map[int32]string, error) {
	root := fmt.Sprintf("%s/brokers/ids", kz.conf.Chroot)
	children, _, err := kz.conn.Children(root)
	if err != nil {
		return nil, err
	}

	type brokerEntry struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}

	result := make(map[int32]string)
	for _, child := range children {
		brokerID, err := strconv.ParseInt(child, 10, 32)
		if err != nil {
			return nil, err
		}

		value, _, err := kz.conn.Get(path.Join(root, child))
		if err != nil {
			return nil, err
		}

		var brokerNode brokerEntry
		if err := json.Unmarshal(value, &brokerNode); err != nil {
			return nil, err
		}

		result[int32(brokerID)] = fmt.Sprintf("%s:%d", brokerNode.Host, brokerNode.Port)
	}

	return result, nil
}

// BrokerList returns a slice of broker addresses that can be used to connect to
// the Kafka cluster, e.g. using `sarama.NewAsyncProducer()`.
func (kz *Kazoo) BrokerList() ([]string, error) {
	brokers, err := kz.Brokers()
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		result = append(result, broker)
	}

	return result, nil
}

// Controller returns what broker is currently acting as controller of the Kafka cluster
func (kz *Kazoo) Controller() (int32, error) {
	type controllerEntry struct {
		BrokerID int32 `json:"brokerid"`
	}

	node := fmt.Sprintf("%s/controller", kz.conf.Chroot)
	data, _, err := kz.conn.Get(node)
	if err != nil {
		return -1, err
	}

	var controllerNode controllerEntry
	if err := json.Unmarshal(data, &controllerNode); err != nil {
		return -1, err
	}

	return controllerNode.BrokerID, nil
}

// Close closes the connection with the Zookeeper cluster
func (kz *Kazoo) Close() error {
	kz.conn.Close()
	return nil
}

////////////////////////////////////////////////////////////////////////
// Util methods
////////////////////////////////////////////////////////////////////////

// Exists checks existence of a node
func (kz *Kazoo) exists(node string) (ok bool, err error) {
	ok, _, err = kz.conn.Exists(node)
	return
}

// DeleteAll deletes a node recursively
func (kz *Kazoo) deleteRecursive(node string) (err error) {
	children, stat, err := kz.conn.Children(node)
	if err == zk.ErrNoNode {
		return nil
	} else if err != nil {
		return
	}

	for _, child := range children {
		if err = kz.deleteRecursive(path.Join(node, child)); err != nil {
			return
		}
	}

	return kz.conn.Delete(node, stat.Version)
}

// MkdirAll creates a directory recursively
func (kz *Kazoo) mkdirRecursive(node string) (err error) {
	parent := path.Dir(node)
	if parent != "/" {
		if err = kz.mkdirRecursive(parent); err != nil {
			return
		}
	}

	_, err = kz.conn.Create(node, nil, 0, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		err = nil
	}
	return
}

// Create stores a new value at node. Fails if already set.
func (kz *Kazoo) create(node string, value []byte, ephemeral bool, seq bool) (err error) {
	if err = kz.mkdirRecursive(path.Dir(node)); err != nil {
		return
	}

	flags := int32(0)
	if ephemeral {
		flags = zk.FlagEphemeral
	}
	if seq {
		flags = flags | zk.FlagSequence
	}
	_, err = kz.conn.Create(node, value, flags, zk.WorldACL(zk.PermAll))
	return
}

// CreateTopic creates new topic use given partitionNum, replicationFactor and topicConfig
func (kz *Kazoo) CreateTopic(topic string, partitions, replicationFactor int, topicConfig map[string]interface{}) (err error) {
	brokerIDs, err := kz.brokerIDs(kz.Brokers())
	if err != nil {
		return
	}
	replicaAssignment, err := kz.assignReplicasToBrokers(brokerIDs, partitions, replicationFactor, -1, -1)
	if err != nil {
		return
	}
	return kz.createOrUpdateTopicPartitionAssignmentPathInZK(topic, replicaAssignment, topicConfig, false)
}

// ExistsTopic checks whether topic exists.
func (kz *Kazoo) ExistsTopic(topic string) (exists bool, err error) {
	exists, err = kz.Topic(topic).Exists()
	if err != nil {
		return
	}
	if exists {
		exists = true
		return
	}
	if strings.Contains(topic, "_") || strings.Contains(topic, ".") {
		var allTopics TopicList
		allTopics, err = kz.Topics()
		if err != nil {
			return
		}
		for _, topicItem := range allTopics {
			if strings.Replace(topicItem.Name, ".", "_", -1) == strings.Replace(topic, ".", "_", -1) {
				exists = true
				return
			}
		}
	}
	return
}

func (kz *Kazoo) createOrUpdateTopicPartitionAssignmentPathInZK(topic string, partitionReplicaAssignment map[int][]int, config map[string]interface{}, update bool) (err error) {

	err = validateTopic(topic)
	if err != nil {
		return
	}

	err = validatePartitionReplicaAssignment(partitionReplicaAssignment)
	if err != nil {
		return
	}

	if !update {
		var exists bool
		exists, err = kz.ExistsTopic(topic)
		if err != nil {
			return
		}
		if exists {
			err = fmt.Errorf("Topic %s already exists.", topic)
			return
		}

		err = kz.writeEntityConfig("topics", topic, config)
		if err != nil {
			return
		}
	}

	assignMap := make(map[string][]int, len(partitionReplicaAssignment))
	for key, value := range partitionReplicaAssignment {
		assignMap[strconv.Itoa(key)] = value
	}

	partitionData := struct {
		Version    int              `json:"version"`
		Partitions map[string][]int `json:"partitions"`
	}{
		Version:    1,
		Partitions: assignMap,
	}

	jsonPartitionData, err := json.Marshal(partitionData)
	if err != nil {
		return
	}

	zkPath := kz.getTopicPath(topic)
	if !update {
		return kz.create(zkPath, []byte(jsonPartitionData), false, false)
	}

	return kz.updatePersistentPath(zkPath, []byte(jsonPartitionData))

}

func validateTopic(topic string) error {
	if len(topic) <= 0 {
		return fmt.Errorf("topic name is illegal, can't be empty")
	}
	if topic == "." || topic == ".." {
		return fmt.Errorf("topic name cannot be . or .. as end")
	}
	if len(topic) > maxTopicNameLength {
		return fmt.Errorf("topic name is illegal, can't be longer than %d characters", maxTopicNameLength)
	}

	if reg := regexp.MustCompile("[a-zA-Z0-9\\._\\-]"); !reg.MatchString(topic) {
		return fmt.Errorf("topic name %s is illegal,  contains a character other than ASCII alphanumerics, '.', '_' and '-'", topic)
	}
	return nil
}

func validatePartitionReplicaAssignment(partitionReplicaAssignment map[int][]int) (err error) {
	replicasSize := -1
	for _, value := range partitionReplicaAssignment {
		curRepSize := len(value)
		if replicasSize != -1 && curRepSize != replicasSize {
			err = fmt.Errorf("All partitions should have the same number of replicas.")
			return
		}
		replicasSize = curRepSize
		seen := make(map[int]struct{}, curRepSize)
		for _, v := range value {
			if _, ok := seen[v]; ok {
				err = fmt.Errorf("Duplicate replica assignment found: %v", partitionReplicaAssignment)
				return
			}
			seen[v] = struct{}{}
		}

	}
	return
}

// ChangeTopicConfig changes the configuration of an existed topic
func (kz *Kazoo) ChangeTopicConfig(topic string, configs map[string]interface{}) (err error) {
	var exists bool
	exists, err = kz.Topic(topic).Exists()
	if err != nil {
		return
	}
	if !exists {
		err = fmt.Errorf("Topic \"%s\" not exists.", topic)
		return
	}
	return kz.changeEntityConfig("topics", topic, configs)
}

func (kz *Kazoo) changeEntityConfig(entityType, entity string, configs map[string]interface{}) (err error) {
	// write the new config--may not exist if there were previously no overrides
	err = kz.writeEntityConfig(entityType, entity, configs)
	if err != nil {
		return
	}

	data := struct {
		Version    int    `json:"version"`
		EntityType string `json:"entity_type"`
		EntityName string `json:"entity_name"`
	}{
		Version:    1,
		EntityType: entityType,
		EntityName: entity,
	}
	var jsonData []byte
	jsonData, err = json.Marshal(data)
	if err != nil {
		return
	}
	err = kz.createPersistentSequential(configChangePath, jsonData)
	return
}

// DeleteTopic deletes an existed topic
func (kz *Kazoo) DeleteTopic(topic string) (err error) {
	err = kz.create(kz.getDeleteTopicPath(topic), []byte{}, false, false)
	return
}

// AddPartitions rearranges partition/replication for an existed topic
func (kz *Kazoo) AddPartitions(topic string, numPartitions int, replicaAssignmentStr string, checkBrokerAvailable bool) (err error) {
	existingPartitionsReplicaList, err := kz.getReplicaAssignmentForTopics(topic)
	if err != nil {
		return
	}
	if len(existingPartitionsReplicaList) == 0 {
		err = fmt.Errorf("The topic %s does not exist", topic)
		return
	}
	var existingReplicaListForPartitionZero []int
	for topicAndPartition, replicas := range existingPartitionsReplicaList {
		if topicAndPartition.Partition == 0 {
			existingReplicaListForPartitionZero = replicas
		}
	}
	if len(existingPartitionsReplicaList) == 0 {
		err = fmt.Errorf("the topic does not have partition with id 0, it should never happen")
		return
	}
	partitionsToAdd := numPartitions - len(existingPartitionsReplicaList)
	if partitionsToAdd <= 0 {
		err = fmt.Errorf("The number of partitions for a topic can only be increased")
		return
	}
	brokerIDs, err := kz.brokerIDs(kz.Brokers())
	if err != nil {
		return
	}
	var newPartitionReplicaList map[int][]int
	if replicaAssignmentStr == "" {
		startIndex := brokerIDs[existingReplicaListForPartitionZero[0]]
		if startIndex < 0 {
			startIndex = 0
		}
		newPartitionReplicaList, err = kz.assignReplicasToBrokers(brokerIDs, partitionsToAdd, len(existingReplicaListForPartitionZero), startIndex, len(existingPartitionsReplicaList))
		if err != nil {
			return
		}
	} else {
		newPartitionReplicaList, err = kz.getManualReplicaAssignment(replicaAssignmentStr, brokerIDs, len(existingPartitionsReplicaList), checkBrokerAvailable)
		if err != nil {
			return
		}
	}
	var unmatchedRepFactorList map[int][]int
	for partition, replicas := range newPartitionReplicaList {
		if len(replicas) != len(existingPartitionsReplicaList) {
			unmatchedRepFactorList[partition] = replicas
		}
	}
	if len(unmatchedRepFactorList) != 0 {
		err = fmt.Errorf("The replication factor in manual replication assignment is not equal to the existing replication factor for the topic %v", len(existingReplicaListForPartitionZero))
		return
	}
	partitionReplicaList := make(map[int][]int, len(existingPartitionsReplicaList))
	for topicPartition, replicas := range existingPartitionsReplicaList {
		partitionReplicaList[topicPartition.Partition] = replicas
	}
	for partition, replicas := range newPartitionReplicaList {
		partitionReplicaList[partition] = replicas
	}
	return kz.createOrUpdateTopicPartitionAssignmentPathInZK(topic, partitionReplicaList, map[string]interface{}{}, true)
}

func (kz *Kazoo) getManualReplicaAssignment(replicaAssignmentList string, availableBrokerList []int, startPartitionID int, checkBrokerAvailable bool) (partitionReplicaList map[int][]int, err error) {
	partitionList := strings.Split(replicaAssignmentList, ",")
	partitionReplicaList = make(map[int][]int)
	partitionID := startPartitionID
	partitionList = partitionList[partitionID:]
	for _, partition := range partitionList {
		brokerList := strings.Split(partition, ":")
		brokerIDs := make([]int, 0, len(brokerList))
		for _, broker := range brokerList {
			var brokerID int
			brokerID, err = strconv.Atoi(broker)
			if err != nil {
				return
			}
			brokerIDs = append(brokerIDs, brokerID)
		}
		if len(brokerIDs) <= 0 {
			err = fmt.Errorf("replication factor must be larger than 0")
			return
		}
		brokerSet := make(map[int]struct{}, len(brokerIDs))
		for _, brokerID := range brokerIDs {
			brokerSet[brokerID] = struct{}{}
		}
		if len(brokerSet) != len(brokerIDs) {
			err = fmt.Errorf("duplicate brokers in replica assignment: %v", brokerList)
			return
		}
		if checkBrokerAvailable {
			for _, broker := range brokerIDs {
				isAvailable := false
				for _, avBroker := range availableBrokerList {
					if avBroker == broker {
						isAvailable = true
					}
				}
				if !isAvailable {
					err = fmt.Errorf("some specified brokers not available. unavailable brokers: %d", broker)
					return
				}
			}
		}
		partitionReplicaList[partitionID] = brokerIDs
		if len(partitionReplicaList[partitionID]) != len(partitionReplicaList[startPartitionID]) {
			err = fmt.Errorf("partition %d  has different replication factor: %v", partitionID, brokerList)
		}
		partitionID++
	}

	return
}

type topicAndPartition struct {
	Topic     string
	Partition int
}

func (kz *Kazoo) getReplicaAssignmentForTopics(topics ...string) (existingPartitionsReplicaList map[topicAndPartition][]int, err error) {
	existingPartitionsReplicaList = make(map[topicAndPartition][]int)
	for _, topic := range topics {
		var data []byte
		path := kz.getTopicPath(topic)
		data, _, err = kz.conn.Get(path)
		if err != nil {
			continue
		}
		var pathData map[string]interface{}
		err = json.Unmarshal(data, &pathData)
		if err != nil {
			continue
		}
		partitionValue, ok := pathData["partitions"]
		if !ok {
			continue
		}
		replicaMap, ok := partitionValue.(map[string]interface{})
		if !ok {
			continue
		}
		for partition, replicas := range replicaMap {
			var (
				partitionID  int
				replicasData []int
			)
			partitionID, err = strconv.Atoi(partition)
			if err != nil {
				return
			}
			rawReplicas, ok := replicas.([]interface{})
			if !ok {
				continue
			}
			for _, rep := range rawReplicas {
				repId, ok := rep.(int)
				if !ok {
					continue
				}
				replicasData = append(replicasData, repId)
			}
			topicAndPartition := topicAndPartition{
				Topic:     topic,
				Partition: partitionID,
			}
			existingPartitionsReplicaList[topicAndPartition] = replicasData
		}
	}
	return
}

func (kz *Kazoo) assignReplicasToBrokers(brokerList []int, nPartitions, replicationFactor, fixedStartIndex, startPartitionID int) (replicaAssignment map[int][]int, err error) {
	if nPartitions <= 0 {
		err = fmt.Errorf("number of partitions must be larger than 0")
		return
	}
	if replicationFactor <= 0 {
		err = fmt.Errorf("replication factor must be larger than 0")
		return
	}
	if replicationFactor > len(brokerList) {
		err = fmt.Errorf("replication factor: %d larger than available brokers: %d", replicationFactor, len(brokerList))
		return
	}
	replicaAssignment = make(map[int][]int)
	startIndex := fixedStartIndex
	if startIndex < 0 {
		startIndex = rand.Intn(len(brokerList))
	}
	currentPartitionID := startPartitionID
	if currentPartitionID < 0 {
		currentPartitionID = 0
	}
	nextReplicaShift := fixedStartIndex
	if nextReplicaShift < 0 {
		nextReplicaShift = rand.Intn(len(brokerList))
	}
	for i := 0; i < nPartitions; i++ {
		if currentPartitionID > 0 && (currentPartitionID%len(brokerList) == 0) {
			nextReplicaShift++
		}
		firstReplicaIndex := (currentPartitionID + startIndex) % len(brokerList)
		replicaList := []int{brokerList[firstReplicaIndex]}
		for j := 0; j < replicationFactor-1; i++ {
			rIndex := replicaIndex(firstReplicaIndex, nextReplicaShift, j, len(brokerList))
			replicaList = append(replicaList, brokerList[rIndex])
		}
		sort.Sort(sort.Reverse(sort.IntSlice(replicaList)))
		replicaAssignment[currentPartitionID] = replicaList
		currentPartitionID++
	}
	return
}

func replicaIndex(firstReplicaIndex, secondReplicaShift, replicaIndex, nBrokers int) int {
	shift := 1 + (secondReplicaShift+replicaIndex)%(nBrokers-1)
	return (firstReplicaIndex + shift) % nBrokers
}

func (kz *Kazoo) brokerIDs(brokers map[int32]string, preError error) (sortBrokerIDs []int, err error) {
	if preError != nil {
		err = preError
		return
	}
	sortBrokerIDs = make([]int, 0, len(brokers))
	for brokerID := range brokers {
		sortBrokerIDs = append(sortBrokerIDs, int(brokerID))
	}
	sort.Ints(sortBrokerIDs)
	return

}

func (kz *Kazoo) writeEntityConfig(entityType, entity string, config map[string]interface{}) (err error) {
	data := struct {
		Version int         `json:"version"`
		Config  interface{} `json:"config"`
	}{
		Version: 1,
		Config:  config,
	}
	var jsonData []byte
	jsonData, err = json.Marshal(data)
	if err != nil {
		return
	}
	return kz.updatePersistentPath(kz.getEntityConfigPath(entityType, entity), []byte(jsonData))
}

func (kz *Kazoo) updatePersistentPath(path string, data []byte) (err error) {
	_, err = kz.conn.Set(path, data, -1)
	if err != nil && err != zk.ErrNoNode {
		return
	}
	err = kz.create(path, data, false, false)
	if err != nil && err != zk.ErrNodeExists {
		return
	}
	_, err = kz.conn.Set(path, data, -1)
	return
}

func (kz *Kazoo) createPersistentSequential(seqNode string, content []byte) (err error) {
	err = kz.create(seqNode, content, false, true)
	return
}

func (kz *Kazoo) getEntityConfigPath(entityType, entity string) string {
	return fmt.Sprintf("%s/%s/%s/%s", kz.conf.Chroot, configRootPath, entityType, entity)
}

func (kz *Kazoo) getDeleteTopicPath(topic string) string {
	return fmt.Sprintf("%s/%s/%s", kz.conf.Chroot, deleteTopicsPath, topic)
}

func (kz *Kazoo) getTopicPath(topic string) string {
	return fmt.Sprintf("%s/%s/%s", kz.conf.Chroot, brokerTopicsPath, topic)
}
