package domain

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalid           = errors.New("invalid input")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrInvalidTransition = errors.New("invalid state transition")
	ErrInvariant         = errors.New("invariant violation")

	ErrWalletPlayerMismatch   = fmt.Errorf("%w: wallet belongs to another player", ErrInvalid)
	ErrWalletCurrencyMismatch = fmt.Errorf("%w: currency differs from wallet currency", ErrInvalid)
)

type FieldError struct {
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

type ValidationError struct {
	Fields []FieldError
}

func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Fields))
	for i, f := range e.Fields {
		parts[i] = f.Field + ": " + f.Reason
	}
	return "invalid input: " + strings.Join(parts, "; ")
}

func (e *ValidationError) Is(target error) bool { return target == ErrInvalid }

func (e *ValidationError) add(field, reason string) {
	e.Fields = append(e.Fields, FieldError{Field: field, Reason: reason})
}

func (e *ValidationError) orNil() error {
	if e == nil || len(e.Fields) == 0 {
		return nil
	}
	return e
}

type Kind string

const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

func ParseKind(s string) (Kind, error) {
	switch k := Kind(s); k {
	case KindOpening, KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return k, nil
	}
	return "", fmt.Errorf("%w: unknown kind %q", ErrInvalid, s)
}

func (k Kind) External() bool { return k != KindOpening && k != "" }
func (k Kind) Reversal() bool { return k == KindRefund || k == KindRollback }

type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

func ParseStatus(s string) (Status, error) {
	switch st := Status(s); st {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return st, nil
	}
	return "", fmt.Errorf("%w: unknown status %q", ErrInvalid, s)
}

func (s Status) Terminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

type Origin string

const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)

type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

func ParseDirection(s string) (Direction, error) {
	switch d := Direction(s); d {
	case Debit, Credit:
		return d, nil
	}
	return "", fmt.Errorf("%w: unknown direction %q", ErrInvalid, s)
}

type FailureCode string

const (
	FailureInsufficientFunds            FailureCode = "INSUFFICIENT_FUNDS"
	FailureInsufficientFundsForReversal FailureCode = "INSUFFICIENT_FUNDS_FOR_REVERSAL"
	FailureReferenceNotFound            FailureCode = "REFERENCE_NOT_FOUND"
	FailureReferenceStillPending        FailureCode = "REFERENCE_STILL_PENDING"
	FailureReferenceNotProcessed        FailureCode = "REFERENCE_NOT_PROCESSED"
	FailureReferenceMismatch            FailureCode = "REFERENCE_MISMATCH"
	FailureReferenceAmountMismatch      FailureCode = "REFERENCE_AMOUNT_MISMATCH"
	FailureInvalidReferenceKind         FailureCode = "INVALID_REFERENCE_KIND"
	FailureAlreadyReversed              FailureCode = "ALREADY_REVERSED"
	FailureProcessingFailed             FailureCode = "PROCESSING_FAILED"
)

func (c FailureCode) rejection() bool {
	switch c {
	case FailureInsufficientFunds, FailureInsufficientFundsForReversal, FailureReferenceNotFound,
		FailureReferenceStillPending, FailureReferenceNotProcessed, FailureReferenceMismatch,
		FailureReferenceAmountMismatch, FailureInvalidReferenceKind, FailureAlreadyReversed:
		return true
	}
	return false
}
