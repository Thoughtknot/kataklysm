package main

import (
	"fmt"
	"kataklysm/pkg/tree"
)

func main() {
	rbt := tree.New[string, int]()
	rbt.Put("dog", 32)
	rbt.Put("dag", 99)
	rbt.Put("dip", 12)
	rbt.Put("arc", 44)
	rbt.Put("dag", 33)
	v, _ := rbt.Get("dog")
	fmt.Println("Query: ", v)
	for it := rbt.Iterator(); it.Next(); {
		fmt.Println(it.Key(), ":", it.Value())
	}
}
