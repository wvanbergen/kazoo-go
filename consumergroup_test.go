package kazoo

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestGenerateConsumerInstanceID(t *testing.T) {
	for i := 0; i < 5; i++ {
		id, err := generateConsumerInstanceID()
		assert.Equal(t, nil, err)
		t.Logf("%+v", id)
	}
}
