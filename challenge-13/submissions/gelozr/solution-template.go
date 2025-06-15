package main

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

// Product represents a product in the inventory system
type Product struct {
	ID       int64
	Name     string
	Price    float64
	Quantity int
	Category string
}

const tableName = "products"

// ProductStore manages product operations
type ProductStore struct {
	db *sql.DB
}

// NewProductStore creates a new ProductStore with the given database connection
func NewProductStore(db *sql.DB) *ProductStore {
	return &ProductStore{db: db}
}

// InitDB sets up a new SQLite database and creates the products table
func InitDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("sql db open: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS ` + tableName + ` (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			price REAL NOT NULL,
			quantity INT NOT NULL,
			category TEXT NOT NULL
		);
	`)
	if err != nil {
		return nil, fmt.Errorf("create products table: %w", err)
	}

	return db, nil
}

// CreateProduct adds a new product to the database
func (ps *ProductStore) CreateProduct(product *Product) error {
	r, err := ps.db.Exec(
		"INSERT INTO "+tableName+" (name, price, quantity, category) VALUES (?, ?, ?, ?);",
		product.Name, product.Price, product.Quantity, product.Category,
	)
	if err != nil {
		return fmt.Errorf("insert product: %w", err)
	}
	product.ID, err = r.LastInsertId()
	if err != nil {
		return fmt.Errorf("get product id: %w", err)
	}
	return nil
}

// GetProduct retrieves a product by ID
func (ps *ProductStore) GetProduct(id int64) (*Product, error) {
	r := ps.db.QueryRow("SELECT * FROM "+tableName+" WHERE id = ?", id)

	var p Product
	err := r.Scan(&p.ID, &p.Name, &p.Price, &p.Quantity, &p.Category)
	if err != nil {
		return nil, fmt.Errorf("get product: %w", err)
	}

	return &p, nil
}

// UpdateProduct updates an existing product
func (ps *ProductStore) UpdateProduct(product *Product) error {
	_, err := ps.GetProduct(product.ID)
	if err != nil {
		return fmt.Errorf("get product: %w", err)
	}

	_, err = ps.db.Exec("UPDATE "+tableName+" SET name = ?, price = ?, quantity = ?, category = ? WHERE id = ?", product.Name, product.Price, product.Quantity, product.Category, product.ID)
	if err != nil {
		return fmt.Errorf("update product: %w", err)
	}

	return nil
}

// DeleteProduct removes a product by ID
func (ps *ProductStore) DeleteProduct(id int64) error {
	_, err := ps.GetProduct(id)
	if err != nil {
		return fmt.Errorf("get product: %w", err)
	}

	_, err = ps.db.Exec("DELETE FROM "+tableName+" WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("delete product: %w", err)
	}
	return nil
}

// ListProducts returns all products with optional filtering by category
func (ps *ProductStore) ListProducts(category string) ([]*Product, error) {
	listQuery := "SELECT * FROM " + tableName

	if category != "" {
		listQuery += " WHERE category = ?"
	}

	var products []*Product
	rows, err := ps.db.Query(listQuery, category)
	if err != nil {
		return nil, fmt.Errorf("list products: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var p Product

		if err = rows.Scan(&p.ID, &p.Name, &p.Price, &p.Quantity, &p.Category); err != nil {
			return nil, fmt.Errorf("list products: %w", err)
		}
		products = append(products, &p)
	}

	return products, nil
}

// BatchUpdateInventory updates the quantity of multiple products in a single transaction
func (ps *ProductStore) BatchUpdateInventory(updates map[int64]int) error {
	tx, err := ps.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	for id, quantity := range updates {
		if _, err = ps.GetProduct(id); err != nil {
			if err = tx.Rollback(); err != nil {
				return fmt.Errorf("rollback: %w", err)
			}
			return fmt.Errorf("get products: %w", err)
		}
		
		if _, err = tx.Exec("UPDATE "+tableName+" SET quantity = ? WHERE id = ?", quantity, id); err != nil {
			if err = tx.Rollback(); err != nil {
				return fmt.Errorf("rollback: %w", err)
			}
			return fmt.Errorf("update products: %w", err)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

func main() {
	// Optional: you can write code here to test your implementation
}
