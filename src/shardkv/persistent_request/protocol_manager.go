package persistent_request

type ProtocolType int

const (
	TWO_PHASE_COMMIT ProtocolType = iota
)

type Message interface {
}

type ProtocolHandler interface {
	HandleMessage(Message)
}

type ProtocolManager struct {
	Protocols map[ProtocolType]ProtocolHandler
}

func DispatchMessage(msg Message) {
	switch msg.(type) {
	default:
		panic("Unhandled message type")
	}
}
