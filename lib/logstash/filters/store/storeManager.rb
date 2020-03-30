# encoding: utf-8

SENSOR_PSQL_STORE="sensor-psql"
WLC_PSQL_STORE="wlc-psql"

class StoreManager
  def self.getKeys(store)
    keys = ["client_mac","namespace_uuid"]

    case store
    when "wlc-psql" 
      keys = ["wireless_station"]
    when "sensor-psql" 
      keys = ["sensor_uuid"]
    when "nmsp-measure" 
      keys = ["client_mac","namespace_uuid"]
    when "nmsp-info" 
      keys = ["client_mac","namespace_uuid"]
    when "radius" 
      keys = ["client_mac","namespace_uuid"]
    when "location" 
      keys = ["client_mac","namespace_uuid"]
    when "dwell" 
      keys = ["client_mac","namespace_uuid"]
    end

    return keys
  end
  
  def self.mustOverwrite(store)
    overwrite = true

    case store
    when "wlc-psql" 
      overwrite = false
    when "sensor-psql" 
      overwrite = false
    when "nmsp-measure" 
      overwrite = false
    when "nmsp-info" 
      overwrite = false
    end

    return overwrite
  end

  def self.enrich(message)
    enrichment = Hash.new
    enrichment.merge!(message)

    storesList = ["wlc-psql","sensor-psql","nmsp-measure","nmsp-info","radius","location","dwell"]

    storesList.each do |store|
      if store.eql?SENSOR_PSQL_STORE or store.sql?WLC_PSQL_STORE
        storeData = @memcache.get(store)
        keys = StoreManager.getKeys(store)
        namespace = message[NAMESPACE_UUID]

        key = enrichment[keys.first] ? keys.first : keys.join
        contents = storeData[key]
        
        if contents
           psqlNamespace = contents[NAMESPACE_UUID]
           if namespace and psqlNamespace
               StoreManager.mustOverwrite(store) ? enrichment.merge!(contents) : enrichment = contents.merge(enrichment) if namespace.eql?psqlNamespace
           else
               StoreManager.mustOverwrite(store) ? enrichment.merge!(contents) : enrichment = contents.merge(enrichment)
           end
        end      
      else
        storeData = @memcache.get(store)
        keys = StoreManger.getKeys(store)
        mergeKey = keys.join
        contents = storeData[mergeKey]
        StoreManager.mustOverwrite(store) ? enrichment.merge!(contents) : enrichment = contents.merge(enrichment) if contents
      end

      return enrichment
    end

  end

end
