package util

import (
	"strings"
)

// BuildAllInfoMap will call `INFO ALL` and return a mapping of map[string]string for each section in the output
func BuildAllInfoMap(infostring string) map[string]map[string]string {
	lines := strings.Split(infostring, "\r\n")
	allmap := make(map[string]map[string]string)
	var sectionname string
	for _, line := range lines {
		if len(line) > 0 {
			if strings.Contains(line, "# ") {
				sectionname = strings.Split(line, "# ")[1]
				allmap[sectionname] = make(map[string]string)
			} else {
				splits := strings.Split(line, ":")
				key := splits[0]
				val := splits[1]
				secmap := allmap[sectionname]
				if secmap == nil {
					allmap[sectionname] = make(map[string]string)
				}
				allmap[sectionname][key] = val
			}
		}
	}
	return allmap
}

// BuildMapFromInfoString will take the string from a Redis info call and
// return a map[string]string
func BuildMapFromInfoString(input string) map[string]string {
	imap := make(map[string]string)
	lines := strings.Split(input, "\r\n")
	for _, line := range lines {
		if len(line) > 0 {
			if strings.Contains(line, "#") {
				imap["section"] = strings.Split(line, "#")[1]
			} else {
				splits := strings.Split(line, ":")
				key := splits[0]
				val := splits[1]
				imap[key] = val
			}
		}
	}
	return imap
}
