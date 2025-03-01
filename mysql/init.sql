CREATE TABLE
  sales_data (
    record_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    sale_date DATE,
    quantity INT,
    unit_price INT,
    total_revenue INT
  );