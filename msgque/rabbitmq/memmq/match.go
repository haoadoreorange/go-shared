package memmq

import "strings"

func match(pattern, key string) bool {
	if pattern == "#" {
		return true
	}
	return backtrack(strings.Split(pattern, "."), strings.Split(key, "."))
}

/*
 * Compare pattern against key left to right, follow AMQP spec.
 * Dots are delimiters, not part of matching — both sides split into words.
 * Wildcards must be standalone words (between dots or at edges)
 *   * backtrack exactly one word       order.* → order.created ✓, order ✗
 *   # backtrack zero or more words     order.# → order ✓, order.a.b ✓
 *   order.create# is literal "create#", not a wildcard
 *
 * Three cases per pattern word
 *   #        — skip zero or more key words (greedy backtrack)
 *   *        — skip one key word (must exist), advance both
 *   literal  — must equal the current key word, advance both
 *
 * # backtracking
 *   # can eat 0, 1, 2, ... N words. We don't know how many upfront.
 *   So we try all possibilities: eat 0 words and check if the rest of
 *   the pattern backtrack the rest of the key. If not, eat 1 word and retry.
 *   Keep going until one works or all fail.
 *
 *   Example: pat=["#","done"] key=["us","east","done"]
 *     eat 0 → backtrack(["done"], ["us","east","done"]) → "done"≠"us" ✗
 *     eat 1 → backtrack(["done"], ["east","done"])      → "done"≠"east" ✗
 *     eat 2 → backtrack(["done"], ["done"])             → "done"=="done" ✓
 *
 * Base case: pattern exhausted. Match only if key is also exhausted
 */
func backtrack(pat, key []string) bool {
	for len(pat) > 0 {
		switch pat[0] {
		case "#":
			if len(pat) == 1 {
				return true // # at end, eat everything remaining
			}
			for i := len(key); i >= 0; i-- { // try eat len(key)..0, faster from the end
				if backtrack(pat[1:], key[i:]) {
					return true
				}
			}
			return false
		case "*":
			if len(key) == 0 {
				return false // nothing to eat
			}
			pat = pat[1:]
			key = key[1:]
		default:
			if len(key) == 0 || pat[0] != key[0] {
				return false
			}
			pat = pat[1:]
			key = key[1:]
		}
	}
	return len(key) == 0
}
