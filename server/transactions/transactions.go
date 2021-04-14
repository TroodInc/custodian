package transactions

type DbTransaction interface {
	Execute([]Operation) error
	Complete() error
	Close() error
	Transaction() interface{}
}
