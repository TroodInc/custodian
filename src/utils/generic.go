package utils

// Contains tells whether a contains x.
func Contains(a []string, x string) bool {
	return IndexOf(a, x) >= 0
}

func IndexOf(a []string, x string) int {
	for i, n := range a {
		if x == n {
			return i
		}
	}
	return -1
}

func Equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
