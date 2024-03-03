package chash

import (
	"testing"
)

func TestHashing1(t *testing.T) {
	hash := New(3, DefaultHash)

	// Given the above hash function, this will give replicas with "hashes":
	// 2, 4, 6, 12, 14, 16, 22, 24, 26
	hash.AddNode("6", "4", "2")

	testCases := map[string]string{
		"2":  "2",
		"11": "2",
		"23": "4",
		"27": "2",
	}

	for k, v := range testCases {
		node, _ := hash.GetNode(k)
		if node != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

	// Adds 8, 18, 28
	hash.AddNode("8")

	// 27 should now map to 8.
	testCases["27"] = "8"

	for k, v := range testCases {
		node, _ := hash.GetNode(k)
		if node != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}
}

func TestHashing2(t *testing.T) {
	hash := New(1, DefaultHash)
	hash.AddNode("node1", "node2", "node3")

	node, _ := hash.GetNode("hello")
	println(node)

	node, _ = hash.GetNode("key2")
	println(node)

	node, _ = hash.GetNode("key3")
	println(node)

	node, _ = hash.GetNode("key4")
	println(node)
}
