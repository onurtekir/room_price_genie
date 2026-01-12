# Data Validation Rules
[Go to README](../../README.md)

RPG pipeline automatically applies `INGESTION`, `LOGIC` and `BUSINESS` level validations while ingesting 
`inventory` and `reservations`

- **Ingestion Level Validation** : Ingestion level validations validates the missing and/or invalid type values 
during the ingestion
- **Logic Level Validation** : Logic level validations validates the logic rules of _ingestion level VALID_ values.
  - Example: `departure_date` must be less than `arrival_date`. To run the logic level validation, `arrival_date` and 
  `departured_date` must be **VALID** date string.
- **Business Level Validation** : Business level validations are performed at the **database level** because of the 
requirement of data integrity. All business level validations are performed within the VIEW named as `view_reservations` 
  - Example: If there are multiple reservations with the same `hotel_id` and `reservation_id`, get the latest one.
  

## Inventory Validation

### Ingestion level validations
🚨 The pipeline **ONLY** allows ingesting one file at a time to maintain the data consistency. 
If there are multiple files inside the `inventory` folder, pipeline rejects them and move the CSV files 
into `archive/error` directory.

- Expected columns : CSV file must contain at least `hotel_id`, `roomt_type_id` and `quantity`
- `hotel_id`
  - Should be **non-empty** valid string.
- `room_type_id`
  - Should be **non-empty** valid string.
- `quantity`
  - Should be valid **integer** or **castable to integer** value. 
  - Should be greater than 0.


## Reservations Validation

The pipeline allows to process multiple reservations JSON files during ingestion. The pipeline is automatically 
calculates the row `SHA-256` hash to prevent writing duplicated entries to the database.

- There are two types of validation(s)
  - **Reservation fields** : If the reservation fields have invalid values, reservation marked as `REJECTED` and stored 
  in `rejected_imports` table
  - **Stay Dates** : Reservation might have multiple stay dates. The pipeline validates every single stay date entry 
  inside the reservation `stay_dates`. If the stay date entry is invalid, pipeline mark the entry as invalid and 
  exclude stay date entry from the reservation. **NOT REJECTING the corresponding reservation**


### Ingestion level validations

#### Reservation fields
- `hotel_id`
  - Must be valid **non-empty** string
- `reservation_id`
  - Must be valid **non-empty** string
- `status`
  - Must ve valid **non-empty** string
  - Value must be one of allowed values: `provisioned`, `waiting_list`, `confirmed`, `cancelled`, `no_show`, 
  `checked_in`, `checked_out`
- `departure_date`
  - Must be valid `YYYY-MM-DD` formatted date string
- `arrival_date`
  - Must be valid `YYYY-MM-DD` formatted date string
- `created_at`
  - Must be valid `UTC ISO-8601` formatted date/time string
- `updated_at`
  - Must be valid `UTC ISO-8601` formatted date/time string
- `stay_dates`
  - `stay_dates` must have at least one entry

#### Stay Dates Entries
- `start_date`
  - Must be valid `YYYY-MM-DD` formatted date string
- `end_date`
  - Must be valid `YYYY-MM-DD` formatted date string
- `room_type_id`
  - Must be valid **non-empty** string
- `room_type_name`
  - Must be valid **non-empty** string
- `number_of_adults`
  - Muste be valid **integer** or **castable to integer** value
  - Must be greater than 0
- `number_of_children`
  - Must be valid **integer** or **castable to integer** value
  - Must be greater than or equal to 0
- `room_revenue_gross_amount`
  - Must be valid **float** or **castable to float** value
  - ⓘ Must be greater than to 0 not implemented. Because, reservation cancellations might be occurred negative revenue 
- `room_revenue_net_amount`
  - Must be valid **float** or **castable to float** value
  - ⓘ Must be greater than to 0 not implemented. Because, reservation cancellations might be occurred negative revenue
- `fnb_gross_amount`
  - Must be valid **float** or **castable to float** value
  - ⓘ Must be greater than to 0 not implemented. Because, reservation cancellations and/or cancelled orders 
  might be occurred negative F&B revenue
- `fnb_net_amount`
  - Must be valid **float** or **castable to float** value
  - ⓘ Must be greater than to 0 not implemented. Because, reservation cancellations and/or cancelled orders 
  might be occurred negative F&B revenue


### Logic level validations

#### Reservation fields
- `arrival_date`, `departure_date`
  - `departure_date` must be greater than `arrival_date`
- `created_at`, `updated_at`
  - `updated_at` must be greater than or equal to `created_at`

#### Stay Dates Entries
- `start_date`, `end_date`
  - `end_date` should be greater than or equal to `start_date`
  - All dates must be fall within reservation period (`arrival_date`, `departure_date`)

### Business level validations
- When multiple entries exist for the same reservation on the same day, the reservation that counts is the last one 
- If the reservation status is `cancelled`, must be excluded from the KPI calculation. **(That can be done within
ingestion level validations. But, to meet the possible requirements in the future to include `cancelled` reservations
 to the KPI calculation, pipeline stores cancelled reservations but excluding KPI calculations.)**
- If the `room_type_id` not exists in the hotel inventory anymore, they should be excluded from the KPI calculation.
- `Each date should appear only once across all stay date objects`. If there are overlapped days, they should be 
excluded from the KPI calculation.
