package api

import (
        "regexp"
        "unicode/utf8"
)

func isValidStyleName(styleName string) bool {
        if len(styleName) <= 0 || len(styleName) > 64 {
                return false
        }
        if !utf8.ValidString(styleName) {
                return false
        }
    isOk, _:= regexp.MatchString("^[.A-Za-z0-9_-]+$", styleName)
    if isOk {
        return true
        }
        return false
}
