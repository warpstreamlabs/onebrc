package main

import "testing"

func TestRun(t *testing.T) {
	Run("measurements.txt", 128, "clickhouse.txt")
}
