/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"fmt"
	"log"
	"time"

	ui "github.com/gizak/termui/v3"
	"github.com/gizak/termui/v3/widgets"
)

const (
	terminalWidth     = 120
	heapAllocBarCount = 6
)

// controller terminal-based controller
type controller struct {
	Grid  *ui.Grid
	Table *widgets.Table
}

func (p *controller) Resize() {
	p.resize()
	ui.Render(p.Grid)
}

func (p *controller) resize() {
	_, h := ui.TerminalDimensions()
	p.Grid.SetRect(0, 0, terminalWidth, h)
}

func (p *controller) initTable() {
	p.Table.Rows = [][]string{
		{"Node", "Precommitted TX ID", "Committed TX ID", "Status"},
	}
	p.Table.TextStyle = ui.NewStyle(ui.ColorWhite)
	p.Table.TextAlignment = ui.AlignLeft
	p.Table.RowSeparator = true
	p.Table.ColumnWidths = []int{30, 30, 30, 30}
	p.Table.PaddingLeft = 2
	p.Table.BorderStyle = ui.NewStyle(ui.ColorGreen)
	p.Table.SetRect(0, 30, 70, 50)
	p.Table.RowStyles[0] = ui.NewStyle(ui.ColorRed, ui.ColorBlack, ui.ModifierBold)
	p.Table.SetRect(2, 2, 60, 10)
}

func (p *controller) Render(states []*State) {
	p.initTable()

	for _, s := range states {
		var status string
		if s.Err == nil {
			status = "Running"
		} else {
			status = s.Err.Error()
		}
		if s.IState != nil {
			p.Table.Rows = append(p.Table.Rows, []string{
				s.Name,
				fmt.Sprint(s.IState.PrecommittedTxId),
				fmt.Sprint(s.IState.TxId),
				status,
			})
		}
	}

	ui.Render(p.Grid)
}

func (p *controller) initUI() {
	p.resize()
	p.Grid.Set(
		ui.NewRow(.4, p.Table),
	)

}

func newController() *controller {
	ctl := &controller{
		Grid:  ui.NewGrid(),
		Table: widgets.NewTable(),
	}

	ctl.initUI()

	return ctl
}

func Run(loader StateLoader, interval time.Duration) {
	if err := ui.Init(); err != nil {
		log.Fatalf("failed to initialize termui: %v", err)
	}
	defer ui.Close()

	controller := newController()

	ev := ui.PollEvents()
	tick := time.Tick(interval)

	for {
		select {
		case e := <-ev:
			switch e.Type {
			case ui.KeyboardEvent:
				// quit on any keyboard event
				return
			case ui.ResizeEvent:
				controller.Resize()
			}
		case <-tick:
			stats := loader.Load()
			// update dashboard every second
			controller.Render(stats)
		}
	}
}
