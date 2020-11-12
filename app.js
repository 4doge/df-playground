const path = require('path');
const DataFrame = require('dataframe-js').DataFrame;

async function loadDataframe() {
  const df = await DataFrame.fromJSON(path.join(__dirname, 'data.json'));
  return df;
}

async function run() {
  const df = await loadDataframe();

  // df.show()

  /**
   * STAT module
   */
  // Finding min/max value
  const min = df.stat.min('qty');
  const max = df.stat.max('qty');

  // Finding average value
  const avg = df.stat.average('qty');

  // Finding mean value
  const mean = df.stat.mean('qty');

  // Finding sum of all values
  const sum = df.stat.sum('qty');

  // Finding variance value
  const var_value = df.stat.var('qty');

  // Finding standard deviation value
  const sd = df.stat.sd('qty');

  // All stats in object format
  const stats = df.stat.stats('qty');
  // console.log(stats);

  /**
   * SQL module
   */
  // We should assign our dataframe to table, which we will query
  df.sql.register('tmp');
  // Request will return dataframe
  const result = DataFrame.sql.request('SELECT * FROM tmp WHERE qty > 15');
  // result.show();

  /**
   * Exporting dataframe as collection/array/dict
   */
  const collection = result.toCollection();
  const array = result.toArray();
  const dict = result.toDict();

  // Selecting specific columns
  const selected = df.select('qty', 'side');
  // selected.show();

  // Type cast on specific column
  const casted = df.cast('qty', String);
  // casted.show();

  // Math operations on specific column
  const mapped = df.map(row => row.set('total', row.get('qty') * row.get('avg_fill_price')));
  // mapped.show();

  // Filter dataframe
  const filtered = df.filter(row => row.get('qty') > 10);
  // filtered.show();

  // Slice dataframe including start, excluding end
  const sliced = df.slice(1, 3);
  // sliced.show();

  // Push new data to dataframe
  const pushed = df.push(
    {
      "id": 5,
      "timestamp": "2020-06-22 14:10:03.346",
      "qty": 11,
      "side": "long",
      "user_id": 1,
      "user_account_id": 1,
      "asset_id": 5,
      "avg_fill_price": 100
    },
    {
      "id": 6,
      "timestamp": "2020-06-22 14:10:03.346",
      "qty": 12,
      "side": "long",
      "user_id": 1,
      "user_account_id": 1,
      "asset_id": 5,
      "avg_fill_price": 100
    }
  );
  // pushed.show();

  // Count rows
  const numRows = df.count();
  // console.log(numRows);

  // Replace values in specified columns
  const replaced = df.replace(10, 200, ['qty', 'avg_fill_price']);
  // replaced.show();

  // Accessing previous or next rows
  const delta = df.map((row, i) => row.set('delta', row.get('qty') - (i > 0 ? df.getRow(i - 1).get('qty') : 0)));
  // delta.show();

  // Sort
  const sorted = df.sortBy('timestamp', true).select('timestamp').toCollection();
  // console.log(sorted);
}

run();

