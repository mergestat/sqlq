package sqlq

import (
	"fmt"
	"os"
)

// LogLevel defines the logging levels used when sending log messages
type LogLevel int

func (i LogLevel) String() string {
	switch i {
	case DebugLevel:
		return "debug"
	case InfoLevel:
		return "info"
	case WarnLevel:
		return "warn"
	case ErrorLevel:
		return "error"
	default:
		return "invalid"
	}
}

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

type LogBackend interface {
	Write(job *Job, level LogLevel, msg string) (int, error)
}

type LogBackendAdapter func(*Job, LogLevel, string) (int, error)

func (fn LogBackendAdapter) Write(job *Job, level LogLevel, msg string) (int, error) {
	return fn(job, level, msg)
}

type Logger struct {
	job *Job
	be  LogBackend
}

// NewLogger returns a new Logger for the given Job,
// which write logs to the provided backend.
func NewLogger(job *Job, be LogBackend) *Logger { return &Logger{job: job, be: be} }

var sp = fmt.Sprintf // just a handy alias

func (log *Logger) Debug(msg string)                       { log.write(DebugLevel, msg) }
func (log *Logger) Debugf(msg string, args ...interface{}) { log.write(DebugLevel, sp(msg, args...)) }
func (log *Logger) Info(msg string)                        { log.write(InfoLevel, msg) }
func (log *Logger) Infof(msg string, args ...interface{})  { log.write(InfoLevel, sp(msg, args...)) }
func (log *Logger) Warn(msg string)                        { log.write(WarnLevel, msg) }
func (log *Logger) Warnf(msg string, args ...interface{})  { log.write(WarnLevel, sp(msg, args...)) }
func (log *Logger) Error(msg string)                       { log.write(ErrorLevel, msg) }
func (log *Logger) Errorf(msg string, args ...interface{}) { log.write(ErrorLevel, sp(msg, args...)) }

func (log *Logger) write(level LogLevel, msg string) {
	if _, err := log.be.Write(log.job, level, msg); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "sqlq: could not write message: %v\n", err)
	}
}
