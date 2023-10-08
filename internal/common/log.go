package common

import (
	"fmt"
	"io"
	"log"
	"runtime"
)

func init() {
	if DefaultLogLevel <= LOG_TRACE {
		log.SetFlags(log.Lmicroseconds)
	}
}

func SetLogger(w io.Writer) {
	log.SetOutput(w)
}

func getCallerInfo() string {
	pc, _, _, _ := runtime.Caller(2)

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
			log.Printf(fmt.Sprintf("%v [INFO] ", fn)+format, args...)
		} else {
			log.Printf("[INFO] "+format, args...)
		}
	}
}

func LWarn(format string, args ...any) {
	if DefaultLogLevel <= LOG_INFO {
		if LogCompleteEnable {
			fn := getCallerInfo()
			log.Printf(fmt.Sprintf("%v [WARN] ", fn)+format, args...)
		} else {
			log.Printf("[WARN] "+format, args...)
		}
	}
}

func LFail(format string, args ...any) {
	if DefaultLogLevel <= LOG_INFO {
		if LogCompleteEnable {
			fn := getCallerInfo()
			log.Printf(fmt.Sprintf("%v [FAIL] ", fn)+format, args...)
		} else {
			log.Printf("[FAIL] "+format, args...)
		}
	}
}
