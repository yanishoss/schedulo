package core

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"time"
)

const (
	maxOpenConns    = 0.8
	maxIdleConns    = 0.35 * maxOpenConns
	maxConnLifetime = time.Minute * 15
)

const (
	mysqlSchema = `
		CREATE TABLE IF NOT EXISTS migrations (
			id INT PRIMARY KEY,
			created_at TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS events (
			id CHAR(36) PRIMARY KEY,
			cron_expression TEXT,
			should_execute_at TIMESTAMP,
			mode TINYINT,
			topic VARCHAR(255),
			payload VARBINARY
		);
	`

	postgresSchema = `
		CREATE TABLE IF NOT EXISTS migrations (
			id INT PRIMARY KEY,
			created_at TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS events (
			id CHAR(36) PRIMARY KEY,
			cron_expression TEXT,
			should_execute_at TIMESTAMP,
			mode SMALLINT,
			topic VARCHAR(255),
			payload BYTEA
		);
	`
)

var migrations = map[int]string{
	1: `ALTER TABLE IF EXISTS events ADD IF NOT EXISTS topic VARCHAR(255);`,
}

var (
	ErrNotFound = errors.New("the requested item cannot be found")
)

type PersistenceManager interface {
	Add(ctx context.Context, e Event) error
	AddBulk(ctx context.Context, evs []Event) error
	Delete(ctx context.Context, id ID) error
	Get(ctx context.Context, id ID) (Event, error)
	GetAll(ctx context.Context) ([]Event, error)
}

type SqlPersistenceManagerConfig struct {
	Url    string
	Driver string
}

type sqlPersistenceManager struct {
	db    *sql.DB
	cache CacheManager
	SqlPersistenceManagerConfig
}

func NewSqlPersistenceManager(cache CacheManager, conf SqlPersistenceManagerConfig) (PersistenceManager, error) {
	db, err := sql.Open(conf.Driver, conf.Url)

	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	m := &sqlPersistenceManager{db: db, cache: cache, SqlPersistenceManagerConfig: conf}

	db.SetMaxOpenConns(int(maxOpenConns * float64(db.Stats().MaxOpenConnections)))
	db.SetMaxIdleConns(int(maxIdleConns * float64(db.Stats().MaxOpenConnections)))
	db.SetConnMaxLifetime(maxConnLifetime)

	if err := m.init(); err != nil {
		return nil, err
	}

	if err := m.runMigrations(); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *sqlPersistenceManager) createTx(ctx context.Context) (*sql.Tx, error) {
	return m.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  false,
	})
}

func (m *sqlPersistenceManager) runMigrations() error {
	tx, err := m.createTx(context.Background())

	if err != nil {
		return err
	}

	migrationNumber := 0

	row := tx.QueryRow("SELECT id FROM migrations ORDER BY created_at DESC LIMIT 1;")

	if err := row.Scan(&migrationNumber); err != nil {
		if err != sql.ErrNoRows {
			if rollErr := tx.Rollback(); rollErr != nil {
				return rollErr
			}

			return err
		}
	}

	defaultMigNumber := migrationNumber

	stmt, ok := migrations[migrationNumber+1]

	for ok {
		_, err := m.db.Exec(stmt)

		if err != nil {
			if rollErr := tx.Rollback(); rollErr != nil {
				return rollErr
			}

			return err
		}

		migrationNumber++
		stmt, ok = migrations[migrationNumber+1]
	}

	if migrationNumber != defaultMigNumber {
		_, err = tx.Exec("INSERT INTO migrations VALUES ($1, $2);", migrationNumber, time.Now())

		if err != nil {
			if rollErr := tx.Rollback(); rollErr != nil {
				return rollErr
			}

			return err
		}
	}

	return tx.Commit()
}

func (m *sqlPersistenceManager) init() error {
	if m.Driver == "postgres" {
		_, err := m.db.Exec(postgresSchema)

		if err != nil {
			return err
		}
	}

	if m.Driver == "mysql" {
		_, err := m.db.Exec(mysqlSchema)

		if err != nil {
			return err
		}
	}

	return nil
}

func (m *sqlPersistenceManager) AddBulk(ctx context.Context, evs []Event) error {
	tx, err := m.createTx(ctx)

	if err != nil {
		return err
	}

	if m.Driver == "postgres" {
		stmt, err := tx.Prepare(pq.CopyIn("events", "id", "cron_expression", "should_execute_at", "mode", "topic", "payload"))

		if err != nil {
			if rollErr := tx.Rollback(); rollErr != nil {
				return rollErr
			}

			return err
		}

		for _, e := range evs {
			_, err = stmt.Exec(e.ID, e.CronExpression, e.ShouldExecuteAt, e.Mode, e.Topic, e.Payload)
			if err != nil {
				if rollErr := tx.Rollback(); rollErr != nil {
					return rollErr
				}

				return err
			}
		}

		_, err = stmt.Exec()

		if err != nil {
			if rollErr := tx.Rollback(); rollErr != nil {
				return rollErr
			}

			return err
		}

		err = stmt.Close()

		if err != nil {
			if rollErr := tx.Rollback(); rollErr != nil {
				return rollErr
			}

			return err
		}

		return tx.Commit()
	}

	if m.Driver == "mysql" {
		q := `INSERT INTO events VALUES`

		args := make([]interface{}, 6*len(evs))

		i := 0
		for _, e := range evs {
			q = fmt.Sprintf("%s ($%d, $%d, $%d, $%d, $%d, $%d)", q, i+1, i+2, i+3, i+4, i+5, i+6)

			args = append(args, e.ID, e.CronExpression, e.ShouldExecuteAt, e.Mode, e.Topic, e.Payload)

			if i < len(evs)-1 {
				q += ",\n"
			}

			i += 6
		}

		q += ";"

		stmt, err := tx.PrepareContext(ctx, q)
		defer stmt.Close()

		if err != nil {
			if rollErr := tx.Rollback(); rollErr != nil {
				return rollErr
			}

			return err
		}

		_, err = stmt.ExecContext(ctx, args...)

		if err != nil {
			if rollErr := tx.Rollback(); rollErr != nil {
				return rollErr
			}

			return err
		}

		err = stmt.Close()

		if err != nil {
			if rollErr := tx.Rollback(); rollErr != nil {
				return rollErr
			}

			return err
		}

		return tx.Commit()
	}

	return nil
}

func (m *sqlPersistenceManager) Add(ctx context.Context, e Event) error {
	tx, err := m.createTx(ctx)

	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `INSERT INTO events VALUES ($1, $2, $3, $4, $5, $6);`, e.ID, e.CronExpression, e.ShouldExecuteAt, e.Mode, e.Topic, e.Payload)

	if err != nil {
		if rollErr := tx.Rollback(); rollErr != nil {
			return rollErr
		}

		return err
	}

	err = m.cache.Add(ctx, e)

	if err != nil {
		if rollErr := tx.Rollback(); rollErr != nil {
			return rollErr
		}

		return err
	}

	return tx.Commit()
}

func (m *sqlPersistenceManager) Delete(ctx context.Context, id ID) error {
	tx, err := m.createTx(ctx)

	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `DELETE FROM events WHERE id = $1;`, string(id))

	if err != nil {
		if rollErr := tx.Rollback(); rollErr != nil {
			return rollErr
		}

		return err
	}

	err = m.cache.Delete(ctx, id)

	if err != nil {
		if rollErr := tx.Rollback(); rollErr != nil {
			return rollErr
		}

		return err
	}

	return tx.Commit()
}

func (m *sqlPersistenceManager) Get(ctx context.Context, id ID) (e Event, err error) {
	e, err = m.cache.Get(ctx, id)

	if err != nil {
		if err == ErrNotFound {
			tx, err := m.createTx(ctx)

			if err != nil {
				return Event{}, err
			}

			row := tx.QueryRowContext(ctx, `SELECT COUNT(id) FROM events WHERE id = $1;`, string(id))
			count := 0

			if err := row.Scan(&count); err != nil {
				if rollErr := tx.Rollback(); rollErr != nil {
					return e, rollErr
				}

				return e, err
			}

			if count == 0 {
				if rollErr := tx.Rollback(); rollErr != nil {
					return e, rollErr
				}

				return e, ErrNotFound
			}

			row = tx.QueryRowContext(ctx, `SELECT * FROM events WHERE id = $1;`, string(id))

			err = row.Scan(
				&e.ID,
				&e.CronExpression,
				&e.ShouldExecuteAt,
				&e.Mode,
				&e.Topic,
				&e.Payload,
			)

			if err != nil {
				if rollErr := tx.Rollback(); rollErr != nil {
					return e, rollErr
				}

				return e, err
			}

			err = m.cache.Add(ctx, e)

			if err != nil {
				if rollErr := tx.Rollback(); rollErr != nil {
					return e, rollErr
				}

				return e, err
			}

			return e, tx.Commit()
		}
	}

	return e, err
}

func (m *sqlPersistenceManager) GetAll(ctx context.Context) (out []Event, err error) {
	tx, err := m.createTx(ctx)

	if err != nil {
		return out, err
	}

	rows, err := tx.QueryContext(ctx, "SELECT * FROM events;")

	if err != nil {
		if rollErr := tx.Rollback(); rollErr != nil {
			return out, rollErr
		}

		return out, err
	}

	for rows.Next() {
		e := Event{}

		if err := rows.Scan(
			&e.ID,
			&e.CronExpression,
			&e.ShouldExecuteAt,
			&e.Mode,
			&e.Topic,
			&e.Payload,
		); err != nil {
			if rollErr := tx.Rollback(); rollErr != nil {
				return out, rollErr
			}

			return out, err
		}

		out = append(out, e)
	}

	err = m.cache.AddBulk(ctx, out)

	if err != nil {
		if rollErr := tx.Rollback(); rollErr != nil {
			return out, rollErr
		}

		return out, err
	}

	return out, tx.Commit()
}
