# encoding: UTF-8
require 'sqlite3'

# you may need to require other libraries here
require 'awesome_print'

module SqliteWrapper
  extend SQLite3
  extend self

  def initialize
    database
  end

  def database
    db = SQLite3::Database.new('db/tmp_delete.db')
  end

  def upsert 
  end

  def create_table(tbl_name, col_names, unique_keys=nil)
    query = unique_keys ? "CREATE TABLE if not exists `#{tbl_name}` (#{col_names.map{|col_name| '`'+col_name.to_s+'`'}.join(',')}, UNIQUE (#{unique_keys.map{|unique_key| '`'+unique_key.to_s+'`'}.join(',')}))" :
      "CREATE TABLE if not exists `#{tbl_name}` (#{col_names.map{|col_name| '`'+col_name.to_s+'`'}.join(',')})"
      database.execute query
  end

  def add_columns(tbl_name, col_name)
    database.execute("ALTER TABLE `#{tbl_name}` ADD COLUMN `#{col_name}`")
  end

  def repsert(unique_keys, tuple, table_name)
    tuple = [tuple] if Hash == tuple.class
    begin
      keys = tuple.first.keys
      selected = tuple.find_all{|item| keys == item.keys }
      insert_or_replace(unique_keys,selected,table_name)
      selected.each{|item| tuple.delete(item)}
      ap tuple
      break if tuple.empty?
    end while(true)
  end

  private
  def insert_or_replace(unique_keys,tuple,table_name)
    prepare_sql = "insert or replace into \
                    `#{table_name}`( #{tuple.first.keys.map{|key| '`'+key.to_s+'`'}.join(",")}) \
                    values(#{tuple.first.keys.length.times.map{'?'}.join(',')})
    "
    database.transaction do |db|
      begin
        db.prepare(prepare_sql) do |statement|
          tuple.each do |row|
            statement.execute row.values
          end
        end
      rescue SQLite3::SQLException => ex
        puts ex.message
        case ex.message
        when /no such table/
          create_table(table_name, tuple.first.keys, unique_keys)
          retry
        when /no column named/
          add_columns(table_name, ex.message.split(/no column named/).last.strip)
          retry
        else
          raise ex
        end
      end
    end
  end
end


db = SqliteWrapper
nha = [{:id=>1,:name=>'John'},{:id=>2,:name=>'Smith'},{:id=>3,:name=>'Mark',:address=>'UK','Current Location'=>'US'},{:id=>4,:name=>'William',:address=>'London','Current Location'=>'India'}]
db.repsert([:id],nha,'dummy_table')
