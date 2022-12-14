package log_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	st := status.New(codes.NotFound, fmt.Sprintf("offset out of range: %d", e.Offset))
	msg := fmt.Sprintf("The requested offset is outside the log's range: %d", e.Offset)
	// send d.Message back to the user (let the processor decide the error message to display)
	d := &errdetails.LocalizedMessage{Locale: "en-US", Message: msg}
	std, err := st.WithDetails(d)
	if err != nil {
		// If this errored, it will always error
		// here, so better panic, so we can figure
		// out why than have this silently passing.
		panic(err)
	}
	return std
}

func (e ErrOffsetOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}
