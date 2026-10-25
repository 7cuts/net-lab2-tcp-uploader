package main

import "time"

func (c *client) updateStatistic() {
	var (
		curTime = time.Now()
		dt      = curTime.Sub(c.prevUpdateTime).Seconds()
		totalT  = curTime.Sub(c.startTime).Seconds()
	)
	c.prevUpdateTime = curTime

	c.instantBps = float64(c.FileSize-c.sent) / dt
	c.avgBps = float64(c.sent) / totalT
}
