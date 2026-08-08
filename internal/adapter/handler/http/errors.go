package http

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/go-playground/validator/v10"
	"github.com/labstack/echo/v4"

	"hopper/internal/domain"
)

type errorResponse struct {
	Error   string   `json:"error"`
	Details []string `json:"details,omitempty"`
}

// NewHTTPErrorHandler maps domain and framework errors to HTTP status codes
// in one place, so handlers never juggle status codes themselves.
func NewHTTPErrorHandler(log *slog.Logger) echo.HTTPErrorHandler {
	return func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}

		status, resp := mapError(err)
		if status >= http.StatusInternalServerError {
			log.ErrorContext(c.Request().Context(), "request failed", "error", err, "path", c.Path(), "status", status)
		}

		var writeErr error
		if c.Request().Method == http.MethodHead {
			writeErr = c.NoContent(status)
		} else {
			writeErr = c.JSON(status, resp)
		}
		if writeErr != nil {
			log.ErrorContext(c.Request().Context(), "writing error response", "error", writeErr)
		}
	}
}

func mapError(err error) (int, errorResponse) {
	var httpErr *echo.HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.Code, errorResponse{Error: fmt.Sprintf("%v", httpErr.Message)}
	}

	var validationErrs validator.ValidationErrors
	if errors.As(err, &validationErrs) {
		details := make([]string, 0, len(validationErrs))
		for _, fe := range validationErrs {
			details = append(details, fmt.Sprintf("%s: failed on %s", fe.Field(), fe.Tag()))
		}
		return http.StatusBadRequest, errorResponse{Error: "validation failed", Details: details}
	}

	switch {
	case errors.Is(err, domain.ErrJobNotFound):
		return http.StatusNotFound, errorResponse{Error: err.Error()}
	case errors.Is(err, domain.ErrInvalidTransition):
		return http.StatusConflict, errorResponse{Error: err.Error()}
	default:
		return http.StatusInternalServerError, errorResponse{Error: "internal server error"}
	}
}
