package main

import (
	"fmt"
	"log"
	"math"
	"net"
	"sort"
	"strings"
	"time"
)

type clientMapKV struct {
	addr   net.Addr
	client client
}

const (
	Reset = "\033[0m"
	// Foreground
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Cyan   = "\033[36m"
	White  = "\033[37m"
	// Styles
	Bold = "\033[1m"
)

const (
	uiUpdateTime     = 1 * time.Second
	uiWaitClientTime = 5 * time.Second
	uiTotalBars      = 10
)

// Delete all clients that was completed "d" ago
// Caller must aquire mutex
func deleteCompletedOrDead(clients map[net.Addr]*client, d time.Duration) (found bool) {
	found = false
	for addr, c := range clients {
		done := c.sent == c.FileSize
		if (done || c.dead) && time.Since(c.prevUpdateTime) > d {
			found = true
			delete(clients, addr)
		}
	}
	return found
}

func ui(s *clientMap) {
	ticker := time.NewTicker(uiUpdateTime)
	for range ticker.C {
		s.mu.Lock()
		pairs := copyClientMap(s)
		deleteCompletedOrDead(s.clients, uiWaitClientTime)
		s.mu.Unlock()
		sort.Slice(
			pairs,
			func(i, j int) bool {
				return pairs[i].addr.String() < pairs[j].addr.String()
			})

		render(pairs)
	}
}

func render(pairs []clientMapKV) {
	fmt.Print("\033[2J\033[3J\033[H") // clear screen
	for _, pair := range pairs {
		c := pair.client
		addr := pair.addr
		fmt.Printf("[%s]", addr)
		fmt.Printf("   avg %s/s", humanizeBytes(c.avgBps))

		if c.instantBps == 0 {
			fmt.Printf("   %sins %s/s%s", Yellow, humanizeBytes(c.instantBps), Reset)
		} else {
			fmt.Printf("   ins %s/s", humanizeBytes(c.instantBps))
		}
		fmt.Printf("   %s", progress(c.sent, c.FileSize))
		fmt.Printf("   %s", c.FileName)
		fmt.Printf(" (%s/%s)", humanizeBytes(float64(c.sent)), humanizeBytes(float64(c.FileSize)))
		fmt.Printf("  %s", colorizeStatus(&pair.client))
		fmt.Printf("\n")
	}
}

// Caller should acquire mutex !
func copyClientMap(s *clientMap) []clientMapKV {
	pairs := make([]clientMapKV, 0, len(s.clients))
	for a, c := range s.clients {
		pairs = append(pairs, clientMapKV{addr: a, client: *c})
	}
	return pairs
}

// returns something like [======     ]  56%
func progress(done int64, total int64) string {

	percent := int(float64(done) / float64(total) * 100)
	doneBars := int(float64(done) / float64(total) * uiTotalBars)

	if total <= 0 {
		percent = 0
		doneBars = 0
	}

	if !(doneBars >= 0 && doneBars <= uiTotalBars) {
		log.Printf("doneBars=%d but total is %d\n", doneBars, uiTotalBars)
		return "[error???]"
	}

	var sb strings.Builder

	if percent == 100 {
		sb.WriteString(Green)
	}

	sb.WriteString("[")
	sb.WriteString(strings.Repeat("=", doneBars))
	sb.WriteString(strings.Repeat(" ", uiTotalBars-doneBars))
	sb.WriteString("]")
	fmt.Fprintf(&sb, " %d%%", percent)

	if percent == 100 {
		sb.WriteString(Reset)
	}

	return sb.String()
}

func humanizeBytes(bytes float64) string {
	if math.Abs(bytes) >= 1024*1024*1024 {
		return fmt.Sprintf("%.1f GB", bytes/1024/1024/1024)
	}
	if math.Abs(bytes) >= 1024*1024 {
		return fmt.Sprintf("%.1f MB", bytes/1024/1024)
	}
	if math.Abs(bytes) >= 1024 {
		return fmt.Sprintf("%.1f KB", bytes/1024)
	}
	return fmt.Sprintf("%.0f B", bytes)
}

func colorizeStatus(c *client) string {

	if c.FileSize == c.sent {
		return Green + "[COMPLETED]" + Reset
	}

	if c.dead {
		return Red + Bold + "[ERROR]" + Reset
	}

	if c.instantBps == 0 {
		return Yellow + "[WAITING]" + Reset
	}
	return Cyan + "[IN PROGRESS]" + Reset
}
