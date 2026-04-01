// Package runtime provides the concurrency primitives for the v2 actor model.
// It is intentionally free of any business-domain imports.
package runtime

import "context"

// ActorBase is a generic actor with a bounded mailbox channel.
// All internal state is owned exclusively by the actor's Run goroutine; no
// external code may access it directly.
//
// Typical embedding:
//
//	type MyActor struct {
//	    runtime.ActorBase[myMsg]
//	    // business fields — only touched inside Run
//	}
type ActorBase[T any] struct {
	mailbox chan T
	done    chan struct{}
}

// NewActorBase constructs an ActorBase with the given mailbox capacity.
func NewActorBase[T any](capacity int) ActorBase[T] {
	return ActorBase[T]{
		mailbox: make(chan T, capacity),
		done:    make(chan struct{}),
	}
}

// Post sends msg non-blockingly (level-triggered: drops silently if full).
// Returns true if the message was enqueued, false if dropped.
func (a *ActorBase[T]) Post(msg T) bool {
	select {
	case a.mailbox <- msg:
		return true
	default:
		return false
	}
}

// PostCtx sends msg and blocks only until ctx is cancelled.
// Use for messages that must not be silently dropped (e.g. stop commands).
func (a *ActorBase[T]) PostCtx(ctx context.Context, msg T) error {
	select {
	case a.mailbox <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RunLoop drives the actor until ctx is cancelled.
// handle is called synchronously for every dequeued message.
func (a *ActorBase[T]) RunLoop(ctx context.Context, handle func(T)) {
	defer close(a.done)
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-a.mailbox:
			handle(msg)
		}
	}
}

// Wait blocks until the actor's run-loop has exited.
func (a *ActorBase[T]) Wait() {
	<-a.done
}

// Done returns the channel that is closed when the actor's run-loop exits.
func (a *ActorBase[T]) Done() <-chan struct{} {
	return a.done
}
