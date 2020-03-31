# encoding: utf-8
require "time"
require "dalli"
require "pg"
require "yaml"

require_relative "util/constants/*"

class PostgresqlManager
  attr_accessor: :enrich_columns, :wlc_sql_store, :store_sensor_sql, 
                 :conn, :enrich_columns, :last_update, :memcached,
                 :stores_to_update

  def initialize(memcached, database,user,pass, port, host)
     self.memcached = memcached
     self.enrich_columns = ["campus", "building", "floor", "deployment",
                           "namespace", "market", "organization", "service_provider", 
                           "zone", "campus_uuid", "building_uuid", "floor_uuid", 
                           "deployment_uuid", "namespace_uuid", "market_uuid", "organization_uuid", 
                           "service_provider_uuid", "zone_uuid"]
     self.wlc_sql_store = self.memcached.get(WLC_PSQL_STORE) || {}
     self.store_sensor_sql = self.memcached.get(SENSOR_PSQL_STORE) || {}
     self.stores_to_update = [WLC_PSQL_STORE, SENSOR_PSQL_STORE]
     self.database = database
     self.user = user
     self.password = password.empty? get_password_from_config_file : password
     self.port = port
     self.host = host
     begin
      self.conn = PG.connect(self.database, self.user, self.pass, self.port, self.host)
     rescue
       puts "DATABASE ERROR: Unable to connect to the database"
     end
     update(true)
     #updateSalts
  end

  def get_password_from_config_file
   pass = ""
   if File.exist?("/opt/rb/var/www/rb-rails/config/database.yml")
         production_config = YAML.load_file("/opt/rb/var/www/rb-rails/config/database.yml")      
         pass = production_config["redborder"]["password"]
   end
   return pass
  end

  def update(force = false)
    return unless need_update? || force
    self.wlc_sql_store = self.memcached.get(WLC_PSQL_STORE) || {}
    self.store_sensor_sql = self.memcached.get(SENSOR_PSQL_STORE) || {}
    self.stores_to_update.each { |store_name| update_store(store_name) }
    self.last_update = Time.now
  end

  def need_update?
    (Time.now.to_i - self.last_update) > (5*60)
  end

  def save_store(store_name)
    return self.memcached.set(WLC_PSQL_STORE,self.wlc_sql_store) if store_name == WLC_PSQL_STORE
    return self.memcached.set(SENSOR_PSQL_STORE, self.store_sensor_sql) if store_name == SENSOR_PSQL_STORE
  end

  def is_number? string
    true if Float(string) rescue false
  end

  def enrich_client_latlong(latitude,longitude)
    location = {}
    if latitude && longitude && is_numeric?latitude && is_numeric?longitude
      longitude_dbl = Float(Math.round(Float(longitude) * 100000) / 100000)
      latitude_dbl = Float(Math.round(Float(latitude) * 100000) / 100000)
      location["client_latlong"] = "#{latitude_dbl},#{longitude_dbl}"
    end
    return location
  end
  
  def enrich_with_columns
    enriching = {}
    self.enrich_columns.each { |column_name|  enriching[column_name] = rs[column_name] if rs[column_name] }
    return enriching
  end

  def update_store(store_name)
    begin 
      result = self.conn.exec(get_sql_query(store_name))
    rescue
      puts "SQL Exception: Error making query"
      return 
    end
    tmpCache = Hash.new
    key = "mac_address" if store_name == WLC_PSQL_STORE
    key = "uuid" if store_name == SENSOR_PSQL_STORE
    result.each do |rs| 
      location = {}
      location.merge!(enrich_client_latlong(rs["latitude"], rs["longitude"]))
      location.merge!(enrich_with_column) if store_name == SENSOR_PSQL_STORE
      
      if rs[key] && !location.empty?
        tmpCache[rs[key]] = location
        self.wlc_sql_store[rs[key]] = location if store_name == WLC_PSQL_STORE
        self.sensor_sql_store[rs[key]] = location if store_name == SENSOR_PSQL_STORE
      end
    end

    self.wlc_sql_store.reject!{ |k,v| !tmpCache.key?k } if store_name == WLC_PSQL_STORE
    self.store_sensor_sql.reject!{ |k,v| !tmpCache.key?k } if store_name == SENSOR_PSQL_STORE
    save_store(store_name)         
  end

  def get_sql_query(store_name)
     return "SELECT uuid, latitude, longitude FROM sensors" if store_name == SENSOR_PSQL_STORE
     return ("SELECT DISTINCT ON (access_points.mac_address) access_points.ip_address, access_points.mac_address, access_points.enrichment," +
            " zones.name AS zone, zones.id AS zone_uuid, access_points.latitude AS latitude, access_points.longitude AS longitude, floors.name AS floor, " +
            " floors.uuid AS floor_uuid, buildings.name AS building, buildings.uuid AS building_uuid, campuses.name AS campus, campuses.uuid AS campus_uuid," +
            " deployments.name AS deployment, deployments.uuid AS deployment_uuid, namespaces.name AS namespace, namespaces.uuid AS namespace_uuid," +
            " markets.name AS market, markets.uuid AS market_uuid, organizations.name AS organization, organizations.uuid AS organization_uuid," +
            " service_providers.name AS service_provider, service_providers.uuid AS service_provider_uuid" +
            " FROM access_points JOIN sensors ON (access_points.sensor_id = sensors.id)" +
            " LEFT JOIN access_points_zones AS zones_ids ON access_points.id = zones_ids.access_point_id" +
            " LEFT JOIN zones ON zones_ids.zone_id = zones.id" +
            " LEFT JOIN (SELECT * FROM sensors WHERE domain_type=101) AS floors ON floors.lft <= sensors.lft AND floors.rgt >= sensors.rgt" +
            " LEFT JOIN (SELECT * FROM sensors WHERE domain_type=5) AS buildings ON buildings.lft <= sensors.lft AND buildings.rgt >= sensors.rgt" +
            " LEFT JOIN (SELECT * FROM sensors WHERE domain_type=4) AS campuses ON campuses.lft <= sensors.lft AND campuses.rgt >= sensors.rgt" +
            " LEFT JOIN (SELECT * FROM sensors WHERE domain_type=7) AS deployments ON deployments.lft <= sensors.lft AND deployments.rgt >= sensors.rgt" +
            " LEFT JOIN (SELECT * FROM sensors WHERE domain_type=8) AS namespaces ON namespaces.lft <= sensors.lft AND namespaces.rgt >= sensors.rgt" +
            " LEFT JOIN (SELECT * FROM sensors WHERE domain_type=3) AS markets ON markets.lft <= sensors.lft AND markets.rgt >= sensors.rgt" +
            " LEFT JOIN (SELECT * FROM sensors WHERE domain_type=2) AS organizations ON organizations.lft <= sensors.lft AND organizations.rgt >= sensors.rgt" +
            " LEFT JOIN (SELECT * FROM sensors WHERE domain_type=6) AS service_providers ON service_providers.lft <= sensors.lft AND service_providers.rgt >= sensors.rgt") if store_name == WLC_PSQL_STORE
  end # end def getSql
end
