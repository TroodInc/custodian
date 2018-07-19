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

func Equal(a, b []string, strictOrder bool) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if strictOrder {
			if v != b[i] {
				return false
			}
		} else {
			vFound := false
			for j := range b {
				if b[j] == v {
					vFound = true
					break
				}
			}
			if !vFound {
				return false
			}
		}
	}
	return true
}

func GetMapKeysValues(targetMap map[string]interface{}) ([]string, []interface{}) {
	values := make([]interface{}, 0)
	keys := make([]string, 0)
	for key, value := range targetMap {
		values = append(values, value)
		keys = append(keys, key)
	}
	return keys, values
}
