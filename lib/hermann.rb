module Hermann
  def self.jruby?
    return RUBY_PLATFORM == "java"
  end
  # Validates that the args are non-blank strings
  #
  # @param [Object] key to validate
  #
  # @param [Object] val to validate
  #
  # @raises [ConfigurationErorr] if either values are empty
  def self.validate_property!(key, val)
    if key.to_s.empty? || val.to_s.empty?
      raise Hermann::Errors::ConfigurationError
    end
  end

  # Packages options into Java Properties object
  #
  # @params [Hash] hash of options to package
  #
  # @return [Properties] packaged java properties
  def self.package_properties(options)
    properties = JavaUtil::Properties.new
    options.each do |key, val|
      Hermann.validate_property!(key, val)
      properties.put(key, val)
    end
    properties
  end
end

if Hermann.jruby?
  require 'hermann/java'
end
