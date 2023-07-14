CREATE TABLE `orders_fact` (
  `order_id` varchar(255) PRIMARY KEY,
  `item_id` varchar(255),
  `customer_id` varchar(255),
  `date_id` integer,
  `discount_id` varchar(255),
  `status` varchar(255),
  `price` decimal,
  `qty_ordered` integer,
  `grand_total` decimal,
  `payment_method` varchar(255)
);

CREATE TABLE `items_dim` (
  `item_id` varchar(255) PRIMARY KEY,
  `sku` varchar(255),
  `category_name` varchar(255)
);

CREATE TABLE `customer_dim` (
  `customer_id` varchar(255) PRIMARY KEY,
  `country` varchar(255)
);

CREATE TABLE `customer_acqusition_fact` (
  `acq_id` varchar(255) PRIMARY KEY,
  `customer_id` varchar(255),
  `date_id` integer
);

CREATE TABLE `discount_dim` (
  `discount_id` varchar(255) PRIMARY KEY,
  `sales_commission_code` varchar(255),
  `discount_amount` decimal
);

CREATE TABLE `datetime_dim` (
  `date_id` integer PRIMARY KEY,
  `init_date` varchar(255),
  `year` integer,
  `month` integer,
  `day` integer,
  `quarter` varchar(255),
  `dayofweek` integer,
  `weekofyear` integer,
  `dayofyear` integer,
  `isweekend` varchar(255)
);

ALTER TABLE `orders_fact` ADD FOREIGN KEY (`item_id`) REFERENCES `items_dim` (`item_id`);

ALTER TABLE `orders_fact` ADD FOREIGN KEY (`customer_id`) REFERENCES `customer_dim` (`customer_id`);

ALTER TABLE `orders_fact` ADD FOREIGN KEY (`date_id`) REFERENCES `datetime_dim` (`date_id`);

ALTER TABLE `orders_fact` ADD FOREIGN KEY (`discount_id`) REFERENCES `discount_dim` (`discount_id`);

ALTER TABLE `customer_dim` ADD FOREIGN KEY (`customer_id`) REFERENCES `customer_acqusition_fact` (`customer_id`);

ALTER TABLE `customer_acqusition_fact` ADD FOREIGN KEY (`date_id`) REFERENCES `datetime_dim` (`date_id`);
