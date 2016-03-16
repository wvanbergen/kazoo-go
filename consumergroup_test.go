package kazoo

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestGenerateConsumerInstanceID(t *testing.T) {
	id, err := generateConsumerInstanceID()
	assert.Equal(t, nil, err)
	t.Logf("%+v", id)
}
