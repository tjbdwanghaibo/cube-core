package mongo

// WriteModel represents a single operation in a bulk write.
type WriteModel struct {
	Type     WriteModelType
	Filter   any
	Document any // for Insert/Replace
	Update   any // for Update
	Upsert   bool
}

// WriteModelType indicates the type of bulk write operation.
type WriteModelType int

const (
	WriteModelInsertOne WriteModelType = iota
	WriteModelUpdateOne
	WriteModelReplaceOne
	WriteModelDeleteOne
)

// NewInsertOneModel creates an insert operation for bulk write.
func NewInsertOneModel(doc any) WriteModel {
	return WriteModel{Type: WriteModelInsertOne, Document: doc}
}

// NewUpdateOneModel creates an update operation for bulk write.
func NewUpdateOneModel(filter, update any, upsert bool) WriteModel {
	return WriteModel{Type: WriteModelUpdateOne, Filter: filter, Update: update, Upsert: upsert}
}

// NewReplaceOneModel creates a replace operation for bulk write.
func NewReplaceOneModel(filter, replacement any, upsert bool) WriteModel {
	return WriteModel{Type: WriteModelReplaceOne, Filter: filter, Document: replacement, Upsert: upsert}
}

// NewDeleteOneModel creates a delete operation for bulk write.
func NewDeleteOneModel(filter any) WriteModel {
	return WriteModel{Type: WriteModelDeleteOne, Filter: filter}
}
