module ETL
  module Processor
    class TemplateSqlProcessor < ETL::Processor::SqlProcessor
      attr_reader :options
      
      ##
      # Perfomrms SqlProcessor task from erb templates
      # source:        Collects empty hash, appends list of sql queires SqlProcessor executes
      # options:       Hash containing variable that will be used in template
      ## file:
      ## template_file
      # source_type:   Filter for hwat is send on source, sql path || sql query
      # logger:        For log results and output
      # db_connection: Database connection to be used
      # show_counts:   No of records inserted, deleted or updated
      ##
      def initialize(control, configuration)
        @options = configuration[:options] || {}
        super
      end

      def process
      @options['files'].each do |file|
        qry_hash = {"#{File.basename(file,".erb")}".sub('template_file',
                    @options['template_file']) => ERB.new(IO.read(file)).result(binding)}
        @source = @source.merge(qry_hash)
      end unless @options['files'].empty?
      super
      end      

    end
  end
end