package registry

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/keyop/keyop-messenger/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- test types -------------------------------------------------------

type orderCreated struct {
	OrderID string `json:"order_id"`
	Amount  int    `json:"amount"`
}

type paymentReceived struct {
	PaymentID string  `json:"payment_id"`
	Total     float64 `json:"total"`
}

// ---- helpers ----------------------------------------------------------

func newReg(t *testing.T) (PayloadRegistry, *testutil.FakeLogger) {
	t.Helper()
	log := &testutil.FakeLogger{}
	return New(log), log
}

func rawJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// ---- Register ---------------------------------------------------------

func TestRegister_ValueType(t *testing.T) {
	reg, _ := newReg(t)
	require.NoError(t, reg.Register("com.keyop.OrderCreated", orderCreated{}))
	assert.Equal(t, []string{"com.keyop.OrderCreated"}, reg.KnownTypes())
}

func TestRegister_PointerType_StoredAsValue(t *testing.T) {
	// Registering a pointer type should be treated identically to the value type.
	reg, _ := newReg(t)
	require.NoError(t, reg.Register("com.keyop.OrderCreated", &orderCreated{}))
	// Decode should return a value, not a pointer.
	raw := rawJSON(t, orderCreated{OrderID: "ord-1", Amount: 10})
	result, err := reg.Decode("com.keyop.OrderCreated", raw)
	require.NoError(t, err)
	_, isValue := result.(orderCreated)
	assert.True(t, isValue, "Decode must return a value type, not a pointer")
}

func TestRegister_NilPrototype(t *testing.T) {
	reg, _ := newReg(t)
	err := reg.Register("some.Type", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "prototype must not be nil")
}

func TestRegister_Duplicate(t *testing.T) {
	reg, _ := newReg(t)
	require.NoError(t, reg.Register("com.keyop.OrderCreated", orderCreated{}))

	err := reg.Register("com.keyop.OrderCreated", orderCreated{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPayloadTypeAlreadyRegistered)
}

// ---- Decode -----------------------------------------------------------

func TestDecode_RegisteredType(t *testing.T) {
	reg, _ := newReg(t)
	require.NoError(t, reg.Register("com.keyop.OrderCreated", orderCreated{}))

	raw := rawJSON(t, orderCreated{OrderID: "ord-42", Amount: 100})
	result, err := reg.Decode("com.keyop.OrderCreated", raw)
	require.NoError(t, err)

	order, ok := result.(orderCreated)
	require.True(t, ok, "result must be type orderCreated, got %T", result)
	assert.Equal(t, "ord-42", order.OrderID)
	assert.Equal(t, 100, order.Amount)
}

func TestDecode_UnregisteredType_ReturnsMap(t *testing.T) {
	reg, log := newReg(t)

	raw := rawJSON(t, map[string]any{"foo": "bar", "n": 1})
	result, err := reg.Decode("com.keyop.Unknown", raw)

	// Must not return an error — message is delivered as map[string]any.
	require.NoError(t, err)
	m, ok := result.(map[string]any)
	require.True(t, ok, "unregistered result must be map[string]any, got %T", result)
	assert.Equal(t, "bar", m["foo"])

	// A warning must have been logged.
	assert.True(t, log.HasWarn("unregistered payload type"),
		"expected a WARN log entry for unregistered type; entries: %v", log.Entries())
}

func TestDecode_UnregisteredType_NullPayload(t *testing.T) {
	reg, _ := newReg(t)
	// null JSON unmarshals into a nil map — should not error.
	result, err := reg.Decode("com.keyop.Unknown", json.RawMessage(`null`))
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestDecode_MalformedJSON(t *testing.T) {
	reg, _ := newReg(t)
	require.NoError(t, reg.Register("com.keyop.OrderCreated", orderCreated{}))

	_, err := reg.Decode("com.keyop.OrderCreated", json.RawMessage(`{invalid`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode payload")
}

func TestDecode_MultipleTypes(t *testing.T) {
	reg, _ := newReg(t)
	require.NoError(t, reg.Register("com.keyop.OrderCreated", orderCreated{}))
	require.NoError(t, reg.Register("com.keyop.PaymentReceived", paymentReceived{}))

	r1, err := reg.Decode("com.keyop.OrderCreated",
		rawJSON(t, orderCreated{OrderID: "o1"}))
	require.NoError(t, err)
	assert.Equal(t, "o1", r1.(orderCreated).OrderID)

	r2, err := reg.Decode("com.keyop.PaymentReceived",
		rawJSON(t, paymentReceived{PaymentID: "p1", Total: 9.99}))
	require.NoError(t, err)
	assert.Equal(t, "p1", r2.(paymentReceived).PaymentID)
}

// ---- KnownTypes -------------------------------------------------------

func TestKnownTypes_Sorted(t *testing.T) {
	reg, _ := newReg(t)
	require.NoError(t, reg.Register("com.keyop.Z", orderCreated{}))
	require.NoError(t, reg.Register("com.keyop.A", orderCreated{}))
	require.NoError(t, reg.Register("com.keyop.M", orderCreated{}))

	types := reg.KnownTypes()
	assert.Equal(t, []string{"com.keyop.A", "com.keyop.M", "com.keyop.Z"}, types)
}

func TestKnownTypes_Empty(t *testing.T) {
	reg, _ := newReg(t)
	assert.Empty(t, reg.KnownTypes())
}

// ---- NilLogger --------------------------------------------------------

func TestNew_NilLogger_DoesNotPanic(t *testing.T) {
	reg := New(nil)
	require.NoError(t, reg.Register("t", orderCreated{}))
	// Decode an unregistered type — this would panic if the nil logger is
	// not replaced with a nop internally.
	assert.NotPanics(t, func() {
		_, _ = reg.Decode("unknown", rawJSON(t, map[string]any{}))
	})
}

// ---- Concurrency ------------------------------------------------------

func TestRegistry_Concurrent(t *testing.T) {
	reg, _ := newReg(t)

	// Pre-register one type so Decode has something to find.
	require.NoError(t, reg.Register("com.keyop.OrderCreated", orderCreated{}))

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Half the goroutines decode the registered type.
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				raw := rawJSON(t, orderCreated{OrderID: "x"})
				result, err := reg.Decode("com.keyop.OrderCreated", raw)
				if err != nil {
					t.Errorf("Decode error: %v", err)
					return
				}
				if _, ok := result.(orderCreated); !ok {
					t.Errorf("unexpected result type: %T", result)
					return
				}
			}
		}()
	}

	// The other half register distinct new types and call KnownTypes.
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			typeStr := fmt.Sprintf("com.keyop.Type%d", i)
			// Ignore duplicate errors — some goroutines may share a suffix.
			_ = reg.Register(typeStr, orderCreated{})
			_ = reg.KnownTypes()
		}()
	}

	wg.Wait()
}
