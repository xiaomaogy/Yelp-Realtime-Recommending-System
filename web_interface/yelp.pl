#!/usr/bin/perl -w

use strict;
use warnings;
use 5.10.0;
use HBase::JSONRest;
use CGI qw/:standard/;
use Data::Dumper;
my $hbase = HBase::JSONRest->new(host => "hadoop-m:2056");
use Encode;

sub cellValue {
	my $row = $_[0];
	my $field_name = $_[1];
	my $row_cells = ${$row}{'columns'};
	foreach my $cell (@$row_cells) {
		if ($$cell{'name'} eq $field_name) {
			return $$cell{'value'};
		}
	}
	return 'missing';
}

my $q = CGI->new;



print header, 
    start_html(-title=>'hello CGI',-head=>Link({-rel=>'stylesheet',-href=>'https://maxcdn.bootstrapcdn.com/bootstrap/3.3.1/css/bootstrap.min.css',-type=>'text/css'})),
    div(
	    {-class =>"container"},
            div(
				{-class => 'row'}, 
				div(
					{-class => 'col-md-12'},
					h1(
						{-class=>"text-center"},
						'Arizona business info powered by yelp data '
					)
				)
			),
			$q->start_form(-role=> 'form',-name => 'main_form',-method =>'GET'),
			div(
				{-class=>'form-group'},
				h5('Find'),
				textfield(
					-class => 'form-control',
					-name => 'keyword',
					-default=> '',
					-size=> 50,
					-maxlength=> 80),
			),
			$q->end_form
		);

my $keyword_str = $q->param('keyword');
$keyword_str = lc $keyword_str;
my @keyword_array = split / /, $keyword_str;
my @business_records_array = ();
my @filtered_business_records_array = ();
my $business_record;
foreach my $keyword(@keyword_array){

	my $inverted_table_record_raw = $hbase->get({
			table => 'yuan_yelp_inverted_table_2',
			where => {key_begins_with => $keyword}
		}
	);
    my @inverted_table_record = @$inverted_table_record_raw;
	my $business_id_array_str = cellValue($inverted_table_record[0],'business:business_id');
	$business_id_array_str  = substr($business_id_array_str,1);
	$business_id_array_str= substr( $business_id_array_str,0,-1);
	my @business_id_array = split /,/, $business_id_array_str;
 	
	foreach my $business_id (@business_id_array){
		$business_record = $hbase->get({
				table => 'yuan_yelp_business_wrc_hbase_sync_2',
				where => {key_begins_with => $business_id}
			}
		);
		push @business_records_array, @$business_record[0];
	}
}

@filtered_business_records_array = @business_records_array;


foreach my $keyword (@keyword_array){
	@filtered_business_records_array = grep {cellValue($_, 'business:categories') =~/$keyword/i or cellValue($_,'business:name') =~/$keyword/i} @filtered_business_records_array;
}

@filtered_business_records_array = sort {cellValue($b, 'business:review_count') <=> cellValue($a, 'business:review_count')} @filtered_business_records_array;

#@filtered_business_records_array = @filtered_business_records_array[0 .. 10]; 
print div({-class=>'container'});
    
print table({-class=>'table table-bordered table-hover', -style=>'width:100%;margin:auto;'},
	Tr(
		th(
			{-class=>'col-md-2 active'},
			['name', 'categories', 'review_count_recorded', 'stars', 'city', 'state', 'review_count_active_user','review_count_all_user','review_count_new']
		)
	)
);

foreach my $row (@filtered_business_records_array){
	print table({-class=>'table borderless table-hover', -style=>'width:100%;margin:auto;'},
		Tr(
			td(
				{-class=>'col-md-1'},
				[cellValue($row, 'business:name'),
					cellValue($row, 'business:categories'),
					cellValue($row, 'business:review_count'),
					cellValue($row, 'business:stars'),
					cellValue($row, 'business:city'),
					cellValue($row, 'business:state'),
					cellValue($row, 'business:review_count_active_user'),
					cellValue($row, 'business:review_count_all_user'),
					cellValue($row, 'business:review_count_new')]
			)

		)
	);
}
#my $in = cellValue(@filtered_business_records_array[0], 'business:review_count_new');
#say Dumper $in."ehre\n";
#my @out =~ tr/\\//d; 
#say Dumper @out;
#say Dumper cellValue(@filtered_business_records_array, 'business:review_count_new');
print end_html;
