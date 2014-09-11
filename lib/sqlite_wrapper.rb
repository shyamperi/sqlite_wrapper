# encoding: UTF-8
require 'sqlite3'

# you may need to require other libraries here
require 'awesome_print'
require 'json'

# Extends Array class with a find_delete_by_keys methods which finds hashes in
# array which matches the keys
class Array
  def find_delete_by_keys(keys)
    select { |item| keys == item.keys }.each { |item| delete(item) }
  end
end

# Extends Sqlite's Database module
module SQLite3
  # Extends sqlite's database class with a repsert and upsert methods
  class Database

    def get_var(key, default)
      d = execute("select value from variables where key=?;",key).flatten.first
      if d.nil?
        return default
      else
        return d
      end
    rescue SQLite3::SQLException => ex
      case ex.message
      when /no such table/
        create_table('variables',['key','value'],['key'])
        retry
      else
        raise ex
      end
    end

    def save_var(key, value)
      d = execute("select count(*) from variables where key=?;",key)[0][0]
      if d .eql? 0
        execute("insert into variables(key,value) values(?,?);",key,value)
      else
        execute("update variables set value=? where key=?;",value,key)
      end
    rescue SQLite3::SQLException => ex
      case ex.message
      when /no such table/
        create_table('variables',['key','value'],['key'])
        retry
      else
        raise ex
      end
    end

    def delete_var(key)
      execute("delete from variables where key=?",key)
    rescue SQLite3::SQLException => ex
      case ex.message
      when /no such table/
        create_table('variables',['key','value'],['key'])
        retry
      else
        raise ex
      end
    end

    def upsert
    end

    def create_table(tbl_name = 'main_table', col_names, unique_keys = nil)
      if unique_keys
        query = "CREATE TABLE if not exists `#{tbl_name}` (#{col_names.map { |col_name| '`' + col_name.to_s + '`' }.join(',')}, UNIQUE (#{unique_keys.map { |unique_key| '`' + unique_key.to_s + '`' }.join(',') }))"
      else
        query = "CREATE TABLE if not exists `#{tbl_name}` (#{col_names.map { |col_name| '`' + col_name.to_s + '`' }.join(',')})"
      end
      execute query
    end

    def add_column(tbl_name = 'main_table', col_name)
      execute("ALTER TABLE `#{tbl_name}` ADD COLUMN `#{col_name}`")
    end

    def repsert(unique_keys, main_tuple, table_name = 'main_table')
      tuple = main_tuple
      tuple = [ tuple ] if Hash == tuple.class
      loop do
        persist(
          unique_keys,
          tuple.find_delete_by_keys(tuple.first.keys),
          table_name
        )
        break if tuple.empty?
      end
    end

    private

    def persist(unique_keys, tuple, table_name)
      prepare_sql = "insert or replace into \
                    `#{table_name}`( #{tuple.first.keys.map { |key| '`' + key.to_s + '`' }.join(',') }) \
                    values(#{tuple.first.keys.length.times.map { '?' }.join(',') } )
      "
      transaction do |db|
        begin
          db.prepare(prepare_sql) do |statement|
            tuple.each do |row|
              statement.execute row.values.map{ |item| 
                case item.class.to_s
                when "Array", "Hash"
                  item.to_json
                else
                  item.to_s
                end
              }
            end
          end
        rescue SQLite3::SQLException => ex
          case ex.message
          when /no such table/
            create_table(table_name, tuple.first.keys, unique_keys)
            retry
          when /no column named/
            add_column(table_name, ex.message.split(/no column named/).last.strip)
            retry
          else
            raise ex
          end
        end
      end
    end
  end
end
