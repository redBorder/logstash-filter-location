# encoding: utf-8

require "logstash/filters/base"
require "logstash/namespace"
require "json"
require "time"
require "dalli"

class LogStash::Filters::Location < LogStash::Filters::Base

  config_name "location"

  #config :path, :validate => :path, :default => " ", :required => false
  
  # Constants.. 
  # Common
  CLIENT_MAC="client_mac"
  WIRELESS_STATION="wireless_station"
  WIRELESS_ID="wireless_id"
  SRC_IP="src_ip"
  SENSOR_IP="sensor_ip"
  DST_IP="dst_ip"
  SENSOR_NAME="sensor_name"
  CLIENT_LATLNG="client_latlong"
  CLIENT_PROFILE="client_profile"
  CLIENT_RSSI="client_rssi"
  CLIENT_RSSI_NUM="client_rssi_num"
  CLIENT_SNR="client_snr"
  CLIENT_SNR_NUM="client_snr_num"
  TIMESTAMP="timestamp"
  FIRST_SWITCHED="first_switched"
  DURATION="duration"
  PKTS="pkts"
  BYTES="bytes"
  TYPE="type"
  SRC_VLAN="src_vlan"
  DST_VLAN="dst_vlan"
  WAN_VLAN="wan_vlan"
  LAN_VLAN="lan_vlan"
  DOT11STATUS="dot11_status"
  CLIENT_MAC_VENDOR="client_mac_vendor"
  CLIENT_ID="client_id"
  SRC_AS_NAME="src_as_name"
  SRC_AS="src_as"
  LAN_IP_AS_NAME="lan_ip_as_name"
  SRC_PORT="src_port"
  LAN_L4_PORT="lan_l4_port"
  SRC_MAP="src_map"
  SRV_PORT="srv_port"
  DST_AS_NAME="dst_as_name"
  WAN_IP_AS_NAME="wan_ip_as_name"
  DST_PORT="dst_port"
  WAN_L4_PORT="wan_l4_port"
  DST_MAP="dst_map"
  DST_AS="dst_as"
  ZONE_UUID="zone_uuid"
  APPLICATION_ID_NAME="application_id_name"
  BIFLOW_DIRECTION="biflow_direction"
  CONVERSATION="conversation"
  DIRECTION="direction"
  ENGINE_ID_NAME="engine_id_name"
  HTTP_HOST="host"
  HTTP_SOCIAL_MEDIA="http_social_media"
  HTTP_SOCIAL_USER="http_social_user"
  HTTP_USER_AGENT_OS="http_user_agent"
  HTTP_REFER_L1="referer"
  IP_PROTOCOL_VERSION="ip_protocol_version"
  L4_PROTO="l4_proto"
  LAN_IP_NET_NAME="lan_ip_net_name"
  SRC_NET_NAME="src_net_name"
  WAN_IP_NET_NAME="wan_ip_net_name"
  DST_NET_NAME="dst_net_name"
  TOS="tos"
  DST_COUNTRY_CODE="dst_country_code"
  WAN_IP_COUNTRY_CODE="wan_ip_country_code"
  SRC_COUNTRY_CODE="src_country_code"
  SRC_COUNTRY="src_country"
  DST_COUNTRY="dst_country"
  LAN_IP_COUNTRY_CODE="lan_ip_country_code"
  SCATTERPLOT="scatterplot"
  INPUT_SNMP="lan_interface_name"
  OUTPUT_SNMP="wan_interface_name"
  INPUT_VRF="input_vrf"
  OUTPUT_VRF="output_vrf"
  SERVICE_PROVIDER="service_provider"
  SERVICE_PROVIDER_UUID="service_provider_uuid"
  SRC="src"
  LAN_IP="lan_ip"
  PUBLIC_IP="public_ip"
  IP_COUNTRY_CODE="ip_country_code"
  IP_AS_NAME="ip_as_name"
  
  BUILDING="building"
  BUILDING_UUID="building_uuid"
  CAMPUS="campus"
  CAMPUS_UUID="campus_uuid"
  FLOOR="floor"
  FLOOR_UUID="floor_uuid"
  ZONE="zone"
  
  COORDINATES_MAP="coordinates_map"
  HNBLOCATION="hnblocation"
  HNBGEOLOCATION="hnbgeolocation"
  RAT="rat"
  DOT11PROTOCOL="dot11_protocol"
  DEPLOYMENT="deployment"
  DEPLOYMENT_UUID="deployment_uuid"
  NAMESPACE="namespace"
  NAMESPACE_UUID="namespace_uuid"
  TIER="tier"
  MSG="msg"
  HTTPS_COMMON_NAME="https_common_name"
  TARGET_NAME="target_name"
  
  CLIENT_FULLNAME="client_fullname"
  PRODUCT_NAME="product_name"
  # #LOC
  LOC_TIMESTAMP_MILLIS="timestampMillis"
  SSID="ssid"
  LOC_MSEUDI="mseUdi"
  LOC_NOTIFICATIONS="notifications"
  LOC_NOTIFICATION_TYPE="notificationType"
  LOC_STREAMING_NOTIFICATION="StreamingNotification"
  LOC_LOCATION="location"
  LOC_GEOCOORDINATEv8="GeoCoordinate"
  LOC_GEOCOORDINATEv9="geoCoordinate"
  LOC_MAPINFOv8="MapInfo"
  LOC_MAPINFOv9="mapInfo"
  LOC_MAPCOORDINATEv8="MapCoordinate"
  LOC_MACADDR="macAddress"
  LOC_MAP_HIERARCHY="mapHierarchyString"
  LOC_MAP_HIERARCHY_V10="locationMapHierarchy"
  LOC_DOT11STATUS="dot11Status"
  LOC_SSID="ssId"
  LOC_IPADDR="ipAddress"
  LOC_AP_MACADDR="apMacAddress"
  LOC_SUBSCRIPTION_NAME="subscriptionName"
  LOC_LONGITUDE="longitude"
  LOC_LATITUDEv8="latitude"
  LOC_LATITUDEv9="lattitude"
  LOC_DEVICEID="deviceId"
  LOC_BAND="band"
  LOC_STATUS="status"
  LOC_USERNAME="username"
  LOC_ENTITY="entity"
  LOC_COORDINATE="locationCoordinate"
  LOC_COORDINATE_X="x"
  LOC_COORDINATE_Y="y"
  LOC_COORDINATE_Z="z"
  LOC_COORDINATE_UNIT="unit"
  WIRELESS_OPERATOR="wireless_operator"
  CLIENT_OS="client_os"
  INTERFACE_NAME="interface_name"
  # LOC LOC
  LOC_ASSOCIATED="ASSOCIATED"
  LOC_PROBING="PROBING"

  #Event
  ACTION="action"
  CLASSIFICATION="classification"
  DOMAIN_NAME="domain_name"
  ETHLENGTH_RANGE="ethlength_range"
  GROUP_NAME="group_name"
  SIG_GENERATOR="sig_generator"
  ICMPTYPE="icmptype"
  IPLEN_RANGE="iplen_range"
  REV="rev"
  SENSOR_UUID="sensor_uuid"
  PRIORITY="priority"
  SIG_ID="sig_id"
  ETHSRC="ethsrc"
  ETHSRC_VENDOR="ethsrc_vendor"
  ETHDST="ethdst"
  ETHDST_VENDOR="ethdst_vendor"
  DST="dst"
  WAN_IP="wan_ip"
  TTL="ttl"
  VLAN="vlan"
  MARKET="market"
  MARKET_UUID="market_uuid"
  ORGANIZATION="organization"
  ORGANIZATION_UUID="organization_uuid"
  CLIENT_LATLONG="client_latlong"
  HASH="hash"
  FILE_SIZE="file_size"
  SHA256="sha256"
  FILE_URI="file_uri"
  FILE_HOSTNAME="file_hostname"
  GROUP_UUID="group_uuid"
  CLIENT_NAME="client_name"

  # Custom
  LOCATION_STORE="location"
  DATASOURCE="rb_location"
  COUNTER_STORE="counterStore"
  FLOWS_NUMBER="flowsNumber"
  # end of Constants
   
  public
  def set_stores
    @store = @memcached.get(LOCATION_STORE)
    @store = Hash.new if @store.nil?
  end

  def register
    @store = {}
    @dimToDruid = [MARKET, MARKET_UUID, ORGANIZATION, ORGANIZATION_UUID,
            DEPLOYMENT, DEPLOYMENT_UUID, SENSOR_NAME, SENSOR_UUID, NAMESPACE, SERVICE_PROVIDER, SERVICE_PROVIDER_UUID]
    options = {:expires_in => 0}
    @memcached = Dalli::Client.new("localhost:11211", options)
    set_stores
  end

  def locv89(event)
    puts "Init locv89"
    namespace_id = (event.get(NAMESPACE_UUID)) ? event.get(NAMESPACE_UUID) : ""
    mseEventContent = event.get(LOC_STREAMING_NOTIFICATION)
    puts mseEventContent
    if (mseEventContent)
      location = mseEventContent[LOC_LOCATION]
      toCache = {}
      toDruid = {}
      
      if (location) 
        geoCoordinate = (location[LOC_GEOCOORDINATEv8]) ? location[LOC_GEOCOORDINATEv8] : location[LOC_GEOCOORDINATEv9]
        mapInfo = (location[LOC_MAPINFOv8]) ? location[LOC_MAPINFOv8] : location[LOC_MAPINFOv9]
        macAddress = String(location[LOC_MACADDR])
        toDruid[CLIENT_MAC] = macAddress
        mapHierarchy = String(mapInfo[LOC_MAP_HIERARCHY])

        if mapHierarchy
          zone = mapHierarchy.split(">")
          toCache[CAMPUS]   = zone[0] if (zone.length >= 1)
          toCache[BUILDING] = zone[1] if (zone.length >= 2)
          toCache[FLOOR]    = zone[2] if (zone.length >= 3)
        end
        
        state = String(location[LOC_DOT11STATUS])
        
        if state
          toDruid[DOT11STATUS] = state
          toCache[DOT11STATUS] = state
        end

        if state and state.eql?LOC_ASSOCIATED
          ip = location[LOC_IPADDR].to_a
          toCache[WIRELESS_ID]      = location[LOC_SSID]       if location[LOC_SSID]
          toCache[WIRELESS_STATION] = location[LOC_AP_MACADDR] if location[LOC_AP_MACADDR]
          toDruid[LAN_IP] = ip.first if ip and ip.first
        end
      end
      
      if geoCoordinate
        latitude = Float( (geoCoordinate[LOC_LATITUDEv8]) ? geoCoordinate[LOC_LATITUDEv8] : geoCoordinate[LOC_LATITUDEv9] )
        latitude = Float ( (latitude * 100000 ).round / 100000 )
 
        longitude = Float(geoCoordinate[LOC_LONGITUDE])
        longitude = Float ( (longitude * 100000 ).round / 100000 )

        locationFormat = latitude.to_s + "," + longitude.to_s
        toCache[CLIENT_LATLNG] = locationFormat 
      end 
      
      dateString = String(mseEventContent[TIMESTAMP]) 
      sensorName = String(mseEventContent[LOC_SUBSCRIPTION_NAME])
     
      toDruid[SENSOR_NAME] = sensorName if sensorName
      
      @dimToDruid.each { |dimension| toDruid[dimension] =  mseEventContent[dimension] if mseEventContent[dimension] }
      @dimToDruid.each { |dimension| toDruid[dimension] =  event.get(dimension) if event.get(dimension) }

      toDruid.merge!(toCache)
      toDruid[CLIENT_RSSI] = "unknown"
      toDruid[CLIENT_SNR] = "unknown"
      toDruid[NAMESPACE_UUID] = namespace_id if !namespace_id.eql?"" 
      toDruid[TYPE] = "mse" 
      toDruid[TIMESTAMP] = (dateString) ? (Time.parse(dateString).to_i / 1000) : (Time.now.to_i / 1000)

      if macAddress
        @store[macAddress + namespace_id] = toCache
        @memcached.set(LOCATION_STORE,@store)
      end
      
      toDruid[CLIENT_PROFILE] = "hard"

      namespace = event.get(NAMESPACE_UUID)
      datasource = (namespace) ? DATASOURCE + "_" + namespace : DATASOURCE

      counterStore = @memcached.get(COUNTER_STORE)
      counterStore = Hash.new if counterStore.nil?
      counterStore[datasource] = counterStore[datasource].nil? ? 0 : (counterStore[datasource] + 1)
      @memcached.set(COUNTER_STORE,counterStore)
      
      flowsNumber = @memcached.get(FLOWS_NUMBER)
      flowsNumber = Hash.new if flowsNumber.nil?
      toDruid["flows_count"] = flowsNumber[datasource] if flowsNumber[datasource] 

      puts "toDruid is: "
      puts toDruid 
      #clean the event
      event.to_hash.each{|k,v| event.remove(k) }
      toDruid.each {|k,v| event.set(k,v)}
    end
  end

  def locv10(event)

  end

  def filter(event)
    puts "Event is: "
    puts event
    puts "Init filter"
    enrichmentEvent = nil
    if (event.get(LOC_STREAMING_NOTIFICATION))
      locv89(event)
    elsif (event.get(LOC_NOTIFICATIONS))
      locv10(event)
    else
      puts "WARN: Unknow location message: {#{event}}"
    end
     
    filter_matched(event)
  end  # def filter
end    # class Logstash::Filter::Location
