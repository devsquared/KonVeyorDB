package main

func assert(predicate bool, message string) {
	if !predicate {
		panic(message)
	}
}
