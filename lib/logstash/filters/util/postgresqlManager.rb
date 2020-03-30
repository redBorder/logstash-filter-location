
require "pg"

class PostgresqlManager
  attr_accessor: :enrichColumns, :storeWLCSql, :storeSensorSql, :conn

  WLC_PSQL_STORE = "wlc-psql"
  SENSOR_PSQL_STORE = "sensor-psql"
  
  @storeWLCSql = storeManager.getStore(WLC_PSQL_STORE);
  @storeSensorSql = storeManager.getStore(SENSOR_PSQL_STORE);

  @enrichColumns = ["campus", "building", "floor", "deployment",
            "namespace", "market", "organization", "service_provider", "zone", "campus_uuid",
            "building_uuid", "floor_uuid", "deployment_uuid", "namespace_uuid", "market_uuid",
            "organization_uuid", "service_provider_uuid", "zone_uuid"]

  def initialize(database,user,pass, port, host)
     self.storeWLCSql = @memcached.get(WLC_PSQL_STORE);
     self.storeSensorSql = @memcached.get(SENSOR_PSQL_STORE);
     self.conn = PG.connect(database, user,pass, port, host)

     update()
     #updateSalts
  end

  def update
    updateWLC
    updateSensor
  end

  def updateWLC
    entries = 0
    self.conn.exec("SELECT DISTINCT ON (access_points.mac_address) access_points.ip_address, access_points.mac_address, access_points.enrichment," +
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
                        " LEFT JOIN (SELECT * FROM sensors WHERE domain_type=6) AS service_providers ON service_providers.lft <= sensors.lft AND service_providers.rgt >= sensors.rgt") do |result|
    
      result.each do |rs|
        location = Hash.new
        enriching = Hash.new
        
        longitude = rs["longitude"]
        latitude = rs["latitude"]
       
        self.enrichColumns.each { |columnName|  enriching[columnName] = rs[columnName] if rs[columnName] }
        entries += 1
        
        if longitude and latitude
          longitudeDbl = Float(Math.round(Float(longigude) * 100000) / 100000)
          latitudeDbl = Float(Math.round(Float(latitude) * 100000) / 100000)
          location["client_latlong" = "#{latitudeDbl},#{longitudeDbl}"        
        end

        location.merge!(enriching)
      end
    end
   
  end

  def updateSensor

  end  

end
