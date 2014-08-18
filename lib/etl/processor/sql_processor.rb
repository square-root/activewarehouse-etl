module ETL
  module Processor
    class SqlProcessor < ETL::Processor::Processor
      attr_reader :source
      attr_reader :source_type
      attr_reader :logger
      attr_reader :db_connection
      attr_reader :show_counts
      attr_reader :place_holders

      ##
      # source:        Collects empty hash, appends list of sql queires SqlProcessor executes
      # source_type:   Filter for hwat is send on source, sql path || sql query
      # logger:        For log results and output
      # db_connection: Database connection to be used
      # show_counts:   No of records inserted, deleted or updated      
      ##
      def initialize(control, configuration)
        @source = configuration[:source] || ''
        @source_type = configuration[:source_type] || 'file'
        @logger = configuration[:logger] || ETL::Engine.logger
        @db_connection = configuration[:db_connection] || ETL::Engine.connection(:datawarehouse)
        @show_counts = configuration[:show_counts] || false
        @place_holders = configuration[:place_holders] || {}
      end
      
      def process
        @source.sort_by{|k,v| k}.each do |sql_name, sql_object|
          sql_object = IO.read(sql_object) if @source_type=='file'
          if sql_name.include?('.template.sql')
            @place_holders.each do |place_holder, replace|
              sql_object.gsub!(place_holder,replace)
            end
          end
          @logger.info "executing #{sql_name}"
          result = execute(sql_object, @db_connection)
          count_rows(sql_name) if @show_counts
          if sql_name.include? 'display'
            @logger.info "Displayed Records: #{result.collect{ |x| x[0]}} " if result.count > 0
          end
          # An empty hook for child class to perform their own processing with result of each query
          post_result_processor(sql_name, result)
        end
      end
      
      private
      ##
      # Displays no of rows changed through sql wrt query file_name
      # Inputs file_name of the query
      # Outputs logger msg with no of rows affected
      ##
      def count_rows(file_name)
        records = @db_connection.select_value('select ROW_COUNT();')
        if file_name.include? 'insert'
          @logger.info "#{records} records loaded."
        elsif file_name.include? 'delete'
          @logger.info "#{records} records deleted."
        elsif file_name.include? 'update'
          @logger.info "#{records} records updated."
        end        
      end

      ##
      # Child Class should implement its own use of sql execute result
      ##
      def post_result_processor(file_name, result)
      end

    end
  end
end