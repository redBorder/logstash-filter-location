# encoding: utf-8
require "dalli"
require_relative "util/constants/*"

class StoreManager
  attr_accessor :memcached

  def initialize(memcached)
    self.memcached = memcached
  end 
  def get_store_keys(store_name)
    return ["wireless_station"] if store_name.eql?WLC_PSQL_STORE
    return ["sensor_uuid"] if store_name.eql?SENSOR_PSQL_STORE
    return ["client_mac","namespace_uuid"]
  end
  
  def must_overwrite?(store_name)
   [ WLC_PSQL_STORE, SENSOR_PSQL_STORE, 
     NMSP_STORE_MEASURE, NMSP_STORE_INFO ].include?store_name ? false : true
  end

  def get_store(store_name)
    self.memcached.get(store_name) || {}
  end

  def enrich(message)
    enrichment = {}
    enrichment.merge!(message)

    stores_list = [ WLC_PSQL_STORE, SENSOR_PSQL_STORE, 
                    NMSP_STORE_MEASURE,NMSP_STORE_INFO,
                    RADIUS_STORE,LOCATION_STORE,DWELL_STORE ]

    stores_list.each do |store_name|
      if store_name.eql?SENSOR_PSQL_STORE or store_name.eql?WLC_PSQL_STORE
        store_data = get_store(store_name)
        keys = get_store_keys(store_name)
        namespace = message[NAMESPACE_UUID]

        key = enrichment[keys.first] ? keys.first : keys.join
        contents = store_data[key]
        
        if contents
           psql_namespace = contents[NAMESPACE_UUID]
           if namespace and psql_namespace
               if namespace.eql?psql_namespace
                 must_overwrite?(store_name) ? enrichment.merge!(contents) : enrichment = contents.merge(enrichment)
               end
           else
               must_overwrite?(store_name) ? enrichment.merge!(contents) : enrichment = contents.merge(enrichment)
           end
        end      
      else
        store_data = get_store(store_name)
        keys = get_store_keys(store_name)
        merge_key = keys.join
        contents = store_data[merge_key]
        must_overwrite?(store_name) ? enrichment.merge!(contents) : enrichment = contents.merge(enrichment) if contents
      end

      return enrichment
    end

  end

end
