package ui

import (
	"context"
	"fmt"
	"pgcr-dataset-processor/internal/parser"
	"sync/atomic"
	"time"

	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/jedib0t/go-pretty/v6/text"
)

type ConsoleRenderer struct {
	StartTime    time.Time
	FileStatuses *parser.StatefulMap
	Counter      *atomic.Int64
	Pw           progress.Writer
	trackers     map[string]*progress.Tracker
}

var (
	StyleColorsExample = progress.StyleColors{
		Message: text.Colors{text.FgWhite},
		Error:   text.Colors{text.FgRed},
		Percent: text.Colors{text.FgHiRed},
		Pinned:  text.Colors{text.BgHiBlack, text.FgWhite, text.Bold},
		Stats:   text.Colors{text.FgHiBlack},
		Time:    text.Colors{text.FgGreen},
		Tracker: text.Colors{text.FgYellow},
		Value:   text.Colors{text.FgCyan},
		Speed:   text.Colors{text.FgMagenta},
	}
)

func NewDisplayOutput(start time.Time, fileStatuses *parser.StatefulMap) ConsoleRenderer {
	renderer := progress.NewWriter()
	renderer.SetAutoStop(false)
	renderer.SetMessageLength(85)
	renderer.SetTrackerPosition(progress.PositionRight)
	renderer.SetUpdateFrequency(150 * time.Millisecond)
	renderer.SetStyle(progress.StyleDefault)
	renderer.SetSortBy(progress.SortByPercentDsc)
	renderer.SetSortBy(progress.SortByMessage)
	renderer.Style().Colors = StyleColorsExample
	renderer.Style().Visibility.ETA = true

	return ConsoleRenderer{
		StartTime:    start,
		FileStatuses: fileStatuses,
		Pw:           renderer,
	}
}

func (o *ConsoleRenderer) DisplayOutput(ctx context.Context) {
	defer o.Pw.Stop()
	go o.Pw.Render()

	for {
		select {
		case <-ctx.Done():
			return
		case file := <-o.FileStatuses.Started:
			go addFileTracker(ctx, o.Pw, file, o.FileStatuses.Data[file].Progress)
		}
	}
}

func addFileTracker(ctx context.Context, writer progress.Writer, file string, lineProgress chan int64) {
	start := time.Now()
	tracker := progress.Tracker{
		Message:            fmt.Sprintf("Procesing file %s", file),
		RemoveOnCompletion: false,
		Units:              progress.UnitsDefault,
		Total:              int64(10_000_000),
	}
	writer.AppendTracker(&tracker)

	for {
		select {
		case <-ctx.Done():
			tracker.UpdateMessage("PGCR processing was interrupted")
			tracker.MarkAsErrored()
		case incrementAmount, ok := <-lineProgress:
			if !ok {
				tracker.UpdateMessage(fmt.Sprintf("Done processing file %s", file))
				tracker.MarkAsDone()
				return
			}
			duration := time.Since(start)
			throughput := float64(tracker.Value()) / float64(duration.Seconds())
			tracker.Increment(incrementAmount)
			tracker.UpdateMessage(fmt.Sprintf("Procesing file %s [Throughput: %.0f pgcrs/sec]", file, throughput))
		}
	}
}
