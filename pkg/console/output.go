package console

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Output struct {
	StartTime time.Time
	Counter   *atomic.Int64
	Done      chan struct{}
}

var (
	totalLines = 10_000_000
	loader     = []rune{'|', '\\', '-', '/'}
)

func (o *Output) DisplayOutput(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	var currentRune int
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-o.Done:
			fmt.Printf("\r✓ Done processing %d lines in %s.\n", o.Counter.Load(), time.Since(o.StartTime).Truncate(time.Second))
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(o.StartTime).Truncate(time.Second)
			current := o.Counter.Load()
			progress := float64(current) / float64(totalLines) * 100
			fmt.Printf("\r%c Processing line %d/%d (%.2f%%) - elapsed: %s", loader[currentRune%len(loader)], current, totalLines, progress, elapsed)
			currentRune++
			time.Sleep(100 * time.Millisecond)
		}
	}
}
