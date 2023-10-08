package common

import (
	"fmt"
	"log"
	"runtime"
)

func init() {
	if DefaultLogLevel <= LOG_TRACE {
		log.SetFlags(log.Lmicroseconds)
	}
}

func getCallerInfo() string {
	pc, _, _, _ := runtime.Caller(1)

	fn := runtime.FuncForPC(pc)
	return fn.Name()
}

func LTrace(format string, args ...any) {
	if DefaultLogLevel <= LOG_TRACE {
		fn := getCallerInfo()
		log.Printf(fn+" [TRACE] "+format, args...)
	}
}

func LInfo(format string, args ...any) {
	if DefaultLogLevel <= LOG_INFO {
		if LogCompleteEnable {
			fn := getCallerInfo()
			log.Printf(fmt.Sprintf("%v [INFO] "+format, fn), args...)
		} else {
			log.Printf("[INFO] "+format, args...)
		}
	}
}

func LWarn(format string, args ...any) {
	if DefaultLogLevel <= LOG_INFO {
		if LogCompleteEnable {
			fn := getCallerInfo()
			log.Printf(fmt.Sprintf("%v [WARN] "+format, fn), args...)
		} else {
			log.Printf("[WARN] "+format, args...)
		}
	}
}

func LFail(format string, args ...any) {
	if DefaultLogLevel <= LOG_INFO {
		if LogCompleteEnable {
			fn := getCallerInfo()
			log.Printf(fmt.Sprintf("%v [FAIL] "+format, fn), args...)
		} else {
			log.Printf("[FAIL] "+format, args...)
		}
	}
}
