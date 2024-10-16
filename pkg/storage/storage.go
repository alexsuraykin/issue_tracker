package storage

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
)

// Storage Хранилище данных.
type Storage struct {
	db *pgxpool.Pool
}

// New Конструктор, принимает строку подключения к БД.
func New(constr string) (*Storage, error) {
	db, err := pgxpool.Connect(context.Background(), constr)
	if err != nil {
		return nil, err
	}
	s := Storage{
		db: db,
	}
	return &s, nil
}

// Task Задача.
type Task struct {
	ID         int
	Opened     int64
	Closed     int64
	AuthorID   int
	AssignedID int
	Title      string
	Content    string
}

// Tasks возвращает список задач из БД.
func (s *Storage) Tasks(taskID, authorID int) ([]Task, error) {
	rows, err := s.db.Query(context.Background(), `
		SELECT 
			id,
			opened,
			closed,
			author_id,
			assigned_id,
			title,
			content
		FROM tasks
		WHERE
			($1 = 0 OR id = $1) AND
			($2 = 0 OR author_id = $2)
		ORDER BY id;
	`,
		taskID,
		authorID,
	)
	if err != nil {
		return nil, err
	}
	var tasks []Task
	// итерирование по результату выполнения запроса
	// и сканирование каждой строки в переменную
	for rows.Next() {
		var t Task
		err = rows.Scan(
			&t.ID,
			&t.Opened,
			&t.Closed,
			&t.AuthorID,
			&t.AssignedID,
			&t.Title,
			&t.Content,
		)
		if err != nil {
			return nil, err
		}
		// добавление переменной в массив результатов
		tasks = append(tasks, t)

	}
	// ВАЖНО не забыть проверить rows.Err()
	return tasks, rows.Err()
}

// NewTask создаёт новую задачу и возвращает её id.
func (s *Storage) NewTask(t Task) (int, error) {
	var id int
	err := s.db.QueryRow(context.Background(), `
		INSERT INTO tasks (title, content)
		VALUES ($1, $2) RETURNING id;
		`,
		t.Title,
		t.Content,
	).Scan(&id)
	return id, err
}

// FindTasksByAuthor ищет задачи по идентификатору автора
func (s *Storage) FindTasksByAuthor(authorID int) ([]Task, error) {
	var tasks []Task
	query := `
		SELECT 
			id,
			opened,
			closed,
			author_id,
			assigned_id,
			title,
			content
		FROM tasks
		WHERE
			$1 = 0 OR author_id = $1
		ORDER BY id;`
	rows, err := s.db.Query(context.Background(), query, authorID)

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var t Task
		err = rows.Scan(
			&t.ID,
			&t.Opened,
			&t.Closed,
			&t.AuthorID,
			&t.AssignedID,
			&t.Title,
			&t.Content,
		)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)

	}
	return tasks, rows.Err()
}

// FindTasksByLabel ищет задачи по идентификатору метки
func (s *Storage) FindTasksByLabel(labelId int) ([]Task, error) {
	query := `SELECT 
					id,
					opened,
					closed,
					author_id,
					assigned_id,
					title,
					content
              FROM tasks
              WHERE id in (SELECT task_id FROM tasks_labels WHERE label_id=$1);`

	rows, err := s.db.Query(context.Background(), query, labelId)

	if err != nil {
		return nil, err
	}

	var tasks []Task

	for rows.Next() {
		var t Task
		err = rows.Scan(
			&t.ID,
			&t.Opened,
			&t.Closed,
			&t.AuthorID,
			&t.AssignedID,
			&t.Title,
			&t.Content,
		)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)

	}
	return tasks, rows.Err()
}

func (s *Storage) UpdateTaskTitleByTaskId(id int, title string) error {
	query := `UPDATE tasks SET title = $2 WHERE id = $1;`
	_, err := s.db.Exec(context.Background(), query, id, title)
	if err != nil {
		return err
	}
	return nil
}

func (s *Storage) UpdateTaskContentByTaskId(id int, content string) error {
	query := `UPDATE tasks SET content = $2 WHERE id = $1;`
	_, err := s.db.Exec(context.Background(), query, id, content)
	if err != nil {
		return err
	}
	return nil
}

func (s *Storage) DeleteTaskByTaskId(id int) error {
	queryDeleteFromTasks := `
		DELETE FROM tasks_labels WHERE task_id = $1
		DELETE FROM tasks WHERE id = $1;
	`
	_, err := s.db.Exec(context.Background(), queryDeleteFromTasks, id)
	if err != nil {
		return err
	}
	return nil
}

func (s *Storage) CloseTaskByTaskId(id int) error {
	query := `UPDATE tasks SET closed = extract(epoch from now()) WHERE id = $1;`
	_, err := s.db.Exec(context.Background(), query, id, close)
	if err != nil {
		return err
	}
	return nil
}
