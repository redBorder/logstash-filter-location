# encoding: utf-8

require "logstash/filters/base"
require "logstash/namespace"
require "json"
require "time"
require "dalli"
require "yaml"

require_relative "util/constants/aggregators"
require_relative "util/constants/constants"
require_relative "util/constants/dimension"
require_relative "util/constants/dimension_value"
require_relative "util/constants/stores"
require_relative "util/postgresql_manager"
require_relative "store/store_manager"

class LogStash::Filters::Location < LogStash::Filters::Base

  config_name "location"

  config :database_name, :validate => :string, :default => "redborder",                    :required => false
  config :user,          :validate => :string, :default => "redborder",                    :required => false
  config :password,      :validate => :string, :default => "",                             :required => false
  config :port,          :validate => :number, :default => "5432",                         :required => false
  config :host,          :validate => :string, :default => "postgresql.redborder.cluster", :required => false
  
  # Custom constants: 
  DATASOURCE="rb_location"
   
  public

  def register
    @dim_to_druid = [MARKET, MARKET_UUID, ORGANIZATION, ORGANIZATION_UUID,
                    DEPLOYMENT, DEPLOYMENT_UUID, SENSOR_NAME, SENSOR_UUID, 
                    NAMESPACE, SERVICE_PROVIDER, SERVICE_PROVIDER_UUID]
    @memcached = Dalli::Client.new("localhost:11211", {:expires_in => 0})
    @store = @memcached.get(LOCATION_STORE) || {}
    @postgresql_manager = PostgresqlManager.new(@memcached, @database_name, @user, @password, @port, @host)
    @store_manager = StoreManager.new(@memcached)
  end

  def locv89(event)
    generated_events = []
    namespace_id = event.get(NAMESPACE_UUID) || ""
    mse_event_content = event.get(LOC_STREAMING_NOTIFICATION)
    if (mse_event_content)
      to_cache = {}
      to_druid = {}
      mac_address = nil
 
      location = mse_event_content[LOC_LOCATION]
      if (location) 
        geo_coordinate = (location[LOC_GEOCOORDINATEv8]) ? location[LOC_GEOCOORDINATEv8] : location[LOC_GEOCOORDINATEv9]
        map_info = (location[LOC_MAPINFOv8]) ? location[LOC_MAPINFOv8] : location[LOC_MAPINFOv9]
        to_druid[CLIENT_MAC] = mac_address = String(location[LOC_MACADDR]) 
        
        map_hierarchy = String(map_info[LOC_MAP_HIERARCHY])
        to_cache[CAMPUS], to_cache[BUILDING], to_cache[FLOOR] = map_hierarchy.split(">") if map_hierarchy
        
        state = String(location[LOC_DOT11STATUS])
        
        to_druid[DOT11STATUS] = to_cache[DOT11STATUS] = state if state

        if state && state == LOC_ASSOCIATED
          ip = location[LOC_IPADDR].to_a
          to_cache[WIRELESS_ID]      = location[LOC_SSID]       if location[LOC_SSID]
          to_cache[WIRELESS_STATION] = location[LOC_AP_MACADDR] if location[LOC_AP_MACADDR]
          to_druid[LAN_IP] = ip.first if ip && ip.first
        end
      end
      
      if geo_coordinate
        latitude = Float((geo_coordinate[LOC_LATITUDEv8]) ? geo_coordinate[LOC_LATITUDEv8] : geo_coordinate[LOC_LATITUDEv9])
        latitude = Float((latitude * 100000 ) / 100000)
 
        longitude = Float(geo_coordinate[LOC_LONGITUDE])
        longitude = Float((longitude * 100000 ) / 100000)

        locationFormat = "%.6f," % latitude + "%.6f" % longitude
        to_cache[CLIENT_LATLNG] = locationFormat 
      end 
      
      date_string = String(mse_event_content[TIMESTAMP]) 
      sensor_name = String(mse_event_content[LOC_SUBSCRIPTION_NAME])
     
      to_druid[SENSOR_NAME] = sensor_name if sensor_name
      
      @dim_to_druid.each { |dimension| to_druid[dimension] =  mse_event_content[dimension] if mse_event_content[dimension] }
      @dim_to_druid.each { |dimension| to_druid[dimension] =  event.get(dimension) if event.get(dimension) }

      to_druid.merge!(to_cache)
      to_druid[CLIENT_RSSI] = to_druid[CLIENT_SNR] = "unknown"
      to_druid[NAMESPACE_UUID] = namespace_id unless namespace_id.empty? 
      to_druid[TYPE] = "mse" 
      to_druid[TIMESTAMP] = (date_string) ? (Time.parse(date_string).to_i / 1000) : (Time.now.to_i / 1000)

      if mac_address
        @store[mac_address + namespace_id] = to_cache
        @memcached.set(LOCATION_STORE,@store)
      end
      
      to_druid[CLIENT_PROFILE] = "hard"

      store_enrichment = @store_manager.enrich(to_druid)
       
      namespace = store_enrichment[NAMESPACE_UUID]
      datasource = (namespace) ? DATASOURCE + "_" + namespace : DATASOURCE

      counter_store = @memcached.get(COUNTER_STORE) || {}
      counter_store[datasource] = counter_store[datasource].nil? ? 0 : (counter_store[datasource] + 1)
      @memcached.set(COUNTER_STORE,counter_store)
      
      flows_number = @memcached.get(FLOWS_NUMBER) || {}
      store_enrichment["flows_count"] = flows_number[datasource] if flows_number[datasource] 

      #clean the event
      enrichment_event = LogStash::Event.new
      store_enrichment.each {|k,v| enrichment_event.set(k,v)}
      generated_events.push(enrichment_event)
      return generated_events
    end
  end

  def process_association(event) 
    messages = event.get("notifications")
    generated_events = []
    if messages
      messages.each do |msg|
        to_cache = {}
        to_druid = {}
        
        client_mac = String(msg[LOC_DEVICEID])
        namespace_id = msg[NAMESPACE_UUID] || ""

        to_cache[WIRELESS_ID] = msg[LOC_SSID] if msg[LOC_SSID]
        to_cache[NMSP_DOT11PROTOCOL] = msg[LOC_BAND] if msg[LOC_BAND]
        to_cache[DOT11STATUS] = msg[LOC_STATUS].to_i if msg[LOC_STATUS]
        to_cache[WIRELESS_STATION] = msg[LOC_AP_MACADDR] if msg[LOC_AP_MACADDR]
        to_cache[CLIENT_ID] = msg[LOC_USERNAME] if msg[LOC_USERNAME]
  
        to_druid.merge!(to_cache)
        
        to_druid[SENSOR_NAME] = msg[LOC_SUBSCRIPTION_NAME]
        to_druid[CLIENT_MAC] = client_mac
        to_druid[TIMESTAMP] = msg[TIMESTAMP].to_i / 1000
        to_druid[TYPE] = "mse10-association"
        to_druid[LOC_SUBSCRIPTION_NAME] = msg[LOC_SUBSCRIPTION_NAME]
        
        to_druid[MARKET] = msg[MARKET] if msg[MARKET]
        to_druid[MARKET_UUID] = msg[MARKET_UUID] if msg[MARKET]
        to_druid[ORGANIZATION] = msg[ORGANIZATION] if msg[ORGANIZATION]
        to_druid[ORGANIZATION_UUID] = msg[ORGANIZATION_UUID] if msg[ORGANIZATION_UUID]
        to_druid[DEPLOYMENT] = msg[DEPLOYMENT] if msg[DEPLOYMENT]
        to_druid[DEPLOYMENT_UUID] = msg[DEPLOYMENT_UUID] if msg[DEPLOYMENT_UUID]
        to_druid[SENSOR_NAME] = msg[SENSOR_NAME] if msg[SENSOR_NAME]
        to_druid[SENSOR_UUID] = msg[SENSOR_UUID] if msg[SENSOR_UUID]
        
        @store[client_mac + namespace_id] = to_cache
        @memcached.set(LOCATION_STORE,@store)
      
        to_druid[CLIENT_PROFILE] = "hard"
       
        store_enrichment = @store_manager.enrich(to_druid)
 
        namespace = store_enrichment[NAMESPACE_UUID]
        datasource = (namespace) ? DATASOURCE + "_" + namespace : DATASOURCE

        counter_store = @memcached.get(COUNTER_STORE) || {}
        counter_store[datasource] = counter_store[datasource].nil? ? 0 : (counter_store[datasource] + 1)
        @memcached.set(COUNTER_STORE,counter_store)

        flows_number = @memcached.get(FLOWS_NUMBER) || {}
        store_enrichment["flows_count"] = flows_number[datasource] if flows_number[datasource]

        #clean the event
        enrichment_event = LogStash::Event.new
        store_enrichment.each {|k,v| enrichment_event.set(k,v)}
        generated_events.push(enrichment_event)
      end
    end
    return generated_events
  end
  def process_location_update(event)
    generated_events = [] 
    messages = event.get("notifications").to_a
    messages.each do |msg|
      to_cache = {}
      to_druid = {}
      
      namespace_id = msg[NAMESPACE_UUID] || ""
      client_mac = String(msg[LOC_DEVICEID])
      location_map_hierarchy = String(msg[LOC_MAP_HIERARCHY_V10])
       
      to_cache[WIRELESS_STATION] = msg[LOC_AP_MACADDR] if msg[LOC_AP_MACADDR]
      to_cache[WIRELESS_ID] = msg[SSID] if msg[SSID]
      
      to_cache[CAMPUS],to_cache[BUILDING],to_cache[FLOOR], to_cache[ZONE] = location_map_hierarchy.split(">") if location_map_hierarchy
      
      assoc_cache = @store[client_mac + namespace_id]
      assoc_cache ? to_cache.merge!(assoc_cache) : to_cache[DOT11STATUS] = "PROBING"

      to_druid.merge!(to_cache)
      to_druid[SENSOR_NAME] = msg[LOC_SUBSCRIPTION_NAME]
      to_druid[LOC_SUBSCRIPTION_NAME] = msg[LOC_SUBSCRIPTION_NAME]
      
      if msg.key?TIMESTAMP
        to_druid[TIMESTAMP] = msg[TIMESTAMP].to_i / 1000
      else
        to_druid[TIMESTAMP] = Time.now.ti_i / 1000
      end

      to_druid[CLIENT_MAC] = client_mac
      to_druid[TYPE] = "mse10-location"
     
      to_druid[NAMESPACE_UUID] = namespace_id unless namespace_id.empty?
 
      @dim_to_druid.each { |dimension| to_druid[dimension] = msg[dimension] if msg[dimension] }
      
      @store[client_mac + namespace_id] = to_cache
      store_enrichment = @store_manager.enrich(to_druid)
      datasource = DATASOURCE
      namespace = store_enrichment[NAMESPACE_UUID]
      datasource = (namespace) ? DATASOURCE + "_" + namespace : DATASOURCE
      
      counter_store = @memcached.get(COUNTER_STORE) || {}
      counter_store[datasource] = counter_store[datasource].nil? ? 0 : (counter_store[datasource] + 1)
      @memcached.set(COUNTER_STORE,counter_store)
      
      flows_number = @memcached.get(FLOWS_NUMBER) || {}
      store_enrichment["flows_count"] = flows_number[datasource] if flows_number[datasource] 

      #clean the event
      enrichment_event = LogStash::Event.new
      store_enrichment.each {|k,v| enrichment_event.set(k,v)}
      generated_events.push(enrichment_event)
    end
  
    return generated_events
  end

  def locv10(event)
    generated_events = []
    notifications = event.get(LOC_NOTIFICATIONS)
    if notifications
      notifications.each do |notification|
        notification_type = notification[LOC_NOTIFICATION_TYPE]
        if notification_type == "association"
          generated_events = process_association(event)
        elsif notification_type == "locationupdate"
          generated_events = process_location_update(event)
        else
          puts "MSE version 10 notificationType is unknown"
        end
      end
    end
    return generated_events
  end

  def filter(event)
    @postgresql_manager.update
    generated_events = []
    if (event.get(LOC_STREAMING_NOTIFICATION))
      generated_events = locv89(event)
    elsif (event.get(LOC_NOTIFICATIONS))
      generated_events = locv10(event)
    else
      puts "WARN: Unknow location message: {#{event}}"
    end

    generated_events.each do |e|
      yield e
    end
    event.cancel
  end  # def filter
end    # class Logstash::Filter::Location
