package console

import (
	"context"
	"fmt"
	"pgcr-dataset-processor/pkg/utils"
	"sync/atomic"
	"time"

	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/jedib0t/go-pretty/v6/text"
)

type ConsoleRenderer struct {
	StartTime    time.Time
	FileStatuses *utils.StatefulMap
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

func NewDisplayOutput(start time.Time, fileStatuses *utils.StatefulMap) ConsoleRenderer {
	renderer := progress.NewWriter()
	renderer.SetAutoStop(false)
	renderer.SetMessageLength(75)
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
				tracker.UpdateMessage(fmt.Sprintf("Done processing file %s. Ok: %v", file, ok))
				tracker.MarkAsDone()
				return
			}
			tracker.Increment(incrementAmount)
			tracker.UpdateMessage(fmt.Sprintf("Procesing file %s. Ok: %v", file, ok))
		}
	}
}
