package core

const maxDefaultStackCapacity = 10000

type StackManagerConfig struct {
	StacksNumber         int
	DefaultStackCapacity int
	MaxStackCapacity     int
}

type stacks []*stack

type stackManager struct {
	stacks     stacks
	config     StackManagerConfig
	insertions int64
}

func newStackManager(config StackManagerConfig) *stackManager {
	if config.MaxStackCapacity == 0 {
		config.MaxStackCapacity = maxDefaultStackCapacity
	}

	stacks := make(stacks, config.StacksNumber)

	for i := 0; i < config.StacksNumber; i++ {
		s := newStack(config.DefaultStackCapacity, config.MaxStackCapacity)
		stacks[i] = &s
	}

	sm := stackManager{
		stacks: stacks,
		config: config,
	}

	return &sm
}

func (s *stackManager) Push(e event) error {
	i := s.getNextStackIndex()
	st := s.stacks[i]

	st.Lock()
	defer st.Unlock()

	if err := st.Push(e); err != nil {
		return err
	}

	s.insertions++

	return nil
}

func (s *stackManager) SetConfig(config StackManagerConfig) error {
	if err := s.resize(config.StacksNumber); err != nil {
		return err
	}

	if config.MaxStackCapacity > s.config.MaxStackCapacity {
		for _, st := range s.stacks {
			st.Lock()
			st.maxCap = config.MaxStackCapacity
			st.Unlock()
		}
	}

	return nil
}

func (s *stackManager) Len() int {
	return stacksLen(s.stacks)
}

func (s *stackManager) resize(stackNumber int) error {
	ln := len(s.stacks)

	if stackNumber == ln {
		return nil
	}

	if stackNumber > ln {
		for i := 0; i < (stackNumber - ln); i++ {
			st := newStack(s.config.DefaultStackCapacity, s.config.MaxStackCapacity)
			s.stacks = append(s.stacks, &st)
		}

		return nil
	}

	if stackNumber < ln {
		if s.config.MaxStackCapacity*stackNumber-s.Len() < 0 {
			return ErrMaxStackCapacity
		}

		delSt := s.stacks[:ln-stackNumber]

		s.stacks = s.stacks[ln-stackNumber:]

		for _, st := range delSt {
			st.Lock()
			for i := 0; i < st.len; i++ {
				if err := s.Push(*st.Get(i).event); err != nil {
					return err
				}
			}
			st.Unlock()
		}
	}

	return nil
}

func (sm *stackManager) getNextStackIndex() int {
	return int((sm.insertions) % int64(len(sm.stacks)))
}

func (s stacks) Len() int {
	return len(s)
}

func stacksLen(s stacks) int {
	i := 0

	for _, st := range s {
		i += st.len
	}

	return i
}
