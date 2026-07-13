package mongo

import "context"

// ICollection is the abstraction for MongoDB collection operations.
type ICollection interface {
	// --- CRUD ---
	InsertOne(ctx context.Context, doc any) (string, error)
	InsertMany(ctx context.Context, docs []any) ([]string, error)
	FindOne(ctx context.Context, filter any, result any) error
	Find(ctx context.Context, filter any, results any, opts ...FindOption) error
	UpdateOne(ctx context.Context, filter any, update any) (*UpdateResult, error)
	UpdateMany(ctx context.Context, filter any, update any) (*UpdateResult, error)
	ReplaceOne(ctx context.Context, filter any, replacement any) (*UpdateResult, error)
	DeleteOne(ctx context.Context, filter any) (int64, error)
	DeleteMany(ctx context.Context, filter any) (int64, error)

	// --- FindAndModify (atomic) ---
	FindOneAndUpdate(ctx context.Context, filter any, update any, result any, opts ...FindOneAndUpdateOption) error
	FindOneAndDelete(ctx context.Context, filter any, result any) error
	FindOneAndReplace(ctx context.Context, filter any, replacement any, result any) error

	// --- Count ---
	CountDocuments(ctx context.Context, filter any) (int64, error)

	// --- Aggregate ---
	Aggregate(ctx context.Context, pipeline any, results any) error

	// --- Bulk ---
	BulkWrite(ctx context.Context, models []WriteModel) (*BulkWriteResult, error)

	// --- Index ---
	EnsureIndexes(ctx context.Context, indexes []IndexModel) error
}

// FindOption configures Find operations.
type FindOption struct {
	Sort  any // sort document (e.g. bson.D{{"created_at", -1}})
	Limit int64
	Skip  int64
}

// FindOneAndUpdateOption configures FindOneAndUpdate operations.
type FindOneAndUpdateOption struct {
	Upsert      bool
	ReturnAfter bool // return document after modification
}

// UpdateResult contains update operation results.
type UpdateResult struct {
	MatchedCount  int64
	ModifiedCount int64
	UpsertedCount int64
	UpsertedID    string
}

// BulkWriteResult contains bulk write results.
type BulkWriteResult struct {
	InsertedCount int64
	MatchedCount  int64
	ModifiedCount int64
	DeletedCount  int64
	UpsertedCount int64
}
