package destination

// Our QueuedQuery interface will be a SQL query and a list of record arguments.
// When we create a new Batch record, we see if the query is the same as the
// previous one, and if so, append the arguments to the existing record in order
// to ensure we have bulk append capabilities.
//
// If the SQL is different, then we just create a new entry
type QueuedQuery struct {
	SQL       string
	Arguments [][]any
}

// Batch queries are a way of bundling multiple queries together to avoid
// unnecessary network round trips. A Batch must only be sent once.
type Batch struct {
	QueuedQueries []*QueuedQuery
}

// Queue queues a query to batch b. query can be an SQL query or the name of a
// prepared statement.  If the statement matches the last one, we append the
// arguments list to this one instead of a redundant entry.
func (b *Batch) Queue(query string, arguments ...any) *QueuedQuery {
	var lastQuery *QueuedQuery = nil

	if len(b.QueuedQueries) > 0 {
		lastQuery = b.QueuedQueries[len(b.QueuedQueries)-1]
	}

	if lastQuery != nil {
		lastQuery.Arguments = append(lastQuery.Arugments, arguments)
	} else {
		qq := &QueuedQuery{
			SQL:       query,
			Arguments: {arguments},
		}
		b.QueuedQueries = append(b.QueuedQueries, qq)
	}
	return nil
}

// Len returns number of queries that have been queued so far.
func (b *Batch) Len() int {
	return len(b.QueuedQueries)
}

type BatchResults interface {
	Close() error
	Exec() error
}

// br := d.conn.SendBatch(ctx, b)
// defer br.Close()

func pivot(matrix [][]any) [][]any {
	if len(matrix) == 0 {
		return [][]any{}
	}

	rowCount := len(matrix)
	colCount := len(matrix[0])
	result := make([][]any, colCount)

	for i := range result {
		result[i] = make([]any, rowCount)
	}

	for i := 0; i < rowCount; i++ {
		for j := 0; j < colCount; j++ {
			result[j][i] = matrix[i][j]
		}
	}

	return result
}

func (b *Batch) PrepareBatch(ctx) {
	for _, query := range b.QueuedQueries {
		query.Arguments = pivot(query.Arguments)
	}
}
