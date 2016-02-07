package kazoo

import (
	"errors"
	"fmt"
)

var (
	ErrTopicExists          = errors.New("Topic already exists")
	ErrTopicMarkedForDelete = errors.New("Topic is already marked for deletion")
)

// CreateTopic creates a new kafka topic with the specified parameters and properties
func (kz *Kazoo) CreateTopic(name string, partitionCount int, replicationFactor int, topicConfig map[string]string) error {
	topic := kz.Topic(name)

	// Official kafka sdk checks if topic exists, then always writes the config unconditionally
	// but only writes the partition map if ones does not exist.
	exists, err := topic.Exists()
	if err != nil {
		return err
	} else if exists {
		return ErrTopicExists
	}

	brokerList, err := kz.BrokerIDList()
	if err != nil {
		return err
	}

	partitionList, err := topic.generatePartitionAssignments(brokerList, partitionCount, replicationFactor)
	if err != nil {
		return err
	}

	configData, err := topic.marshalConfig(topicConfig)
	if err != nil {
		return err
	}

	partitionData, err := topic.marshalPartitions(partitionList)
	if err != nil {
		return err
	}

	if err = kz.createOrUpdate(topic.getConfigPath(), configData, false); err != nil {
		return err
	}

	if err = kz.create(topic.getMetadataPath(), partitionData, false); err != nil {
		return err
	}

	return nil
}

// DeleteTopic marks a kafka topic for deletion. Deleting a topic is asynchronous and
// DeleteTopic will return before Kafka actually does the deletion.
func (kz *Kazoo) DeleteTopic(name string) error {
	node := fmt.Sprintf("%s/admin/delete_topics/%s", kz.conf.Chroot, name)

	exists, err := kz.exists(node)
	if err != nil {
		return err
	}
	if exists {
		return ErrTopicMarkedForDelete
	}

	if err := kz.create(node, nil, false); err != nil {
		return err
	}
	return nil
}
