
=head1 NAME

EPrints::Plugin::Import::RJ_Broker_2

=head1 DESCRIPTION

A SWORD 2 Importer for the Repository Junction Broker.

The RJ_Broker package is a zip file, with 0+ files in 0+ directories, and a mets.xml file in the root directory.
The metadata in the XML file is described at L<http://broker.edina.ac.uk/data_format.html>

This package unpacks the .zip file and creates a complete eprint (metadata and 0+ files) from the single I<create> function.

=cut

package EPrints::Plugin::Import::RJ_Broker_2;

use strict;

use EPrints::Plugin::Import::Binary;
use EPrints::Plugin::Import::Archive;
use EPrints::Plugin::Import::RJ_Broker_parser;
use Data::Dumper;

use XML::LibXML 1.63;
use Archive::Extract;

our @ISA = qw/ EPrints::Plugin::Import::Archive
    EPrints::Plugin::Import::RJ_Broker_parser /;

#===  CLASS METHOD  ===========================================================
#        CLASS:  RJ_Broker_2
#       METHOD:  new
#   PARAMETERS:  %params
#      RETURNS:  $plugin object
#  DESCRIPTION:  Creates a new importer plugin object and registers it with
#                EPrints
#       THROWS:  no exceptions
#     COMMENTS:  none
#     SEE ALSO:  n/a
#==============================================================================
sub new {
    my ( $class, %params ) = @_;

    my $self = $class->SUPER::new(%params);

    # The name to display
    $self->{name} = "RJ_Broker package";

    # Who can see it, and whether we show it
    $self->{visible}   = "all";
    $self->{advertise} = 1;

    # What the importer produces
    $self->{produce} = [qw( list/eprint dataobj/eprint )];

    # What mime-types can trigger this importer
    $self->{accept} = [
        "application/vnd.rjbroker", "sword:http://opendepot.org/broker/1.0"
    ];

    # what actions to take with the data supplied
    $self->{actions} = [qw( metadata media )];

    return $self;
} ## end sub new

#===  CLASS METHOD  ============================================================
#        CLASS:  RJ_Broker_3
#       METHOD:  input_fh
#   PARAMETERS:  %opts - options
#      RETURNS:  an EPrint::List of imported records
#  DESCRIPTION:  This is main import routine: it unpacks the file at the end
#                of the filehandle; reads in the mets.xml file & creates the
#                metadata; attaches each file in each directory; and then creates
#                an eprint-object from that record.
#       THROWS:  no exceptions
#     COMMENTS:  none
#     SEE ALSO:  RJ_Broker_parser
#===============================================================================
##        $opts{fh} = filehandle for deposited file;
##        $opts{actions} = [list of actions]
##        $opts{mime_type} = $headers->{content_type};
##        $opts{dataset} = dataset for the object (eprint, document, etc);
##        $opts{filename} = name of the file
sub input_fh {
    my ( $plugin, %opts ) = @_;

    my $fh      = $opts{fh};
    my $dataset = $opts{dataset};
    my $repo    = $plugin->{session};

    my @ids;

    # get the type of file (should be zip) and the filename ($zipfile) for
    # the file just deposited.
    # (local method)
    my ( $type, $zipfile ) = $plugin->upload_archive($fh);

    # This is localised simply so we can have a bunch of variables to deduce
    # the METS file I want (I know the file will be called mets.xml, and I
    # know it will be at the top of the tree.
    # It also barfs if we can't unzip the file given.
    {
        my $ae = Archive::Extract->new( archive => $zipfile, type => 'zip' );

        my $tmp_dir = File::Temp->newdir();

        if ( !defined $tmp_dir ) {
            my $message
                = "\n[SWORD-DEPOSIT] [INTERNAL-ERROR] Failed to create the temp directory!";
            $repo->log($message);
            EPrints::Apache::AnApache::send_status_line( $repo->{"request"},
                500, $message );
            return;
        } ## end if ( !defined $tmp_dir)

        ## extract files into $tmp_dir
        my $ok = $ae->extract( to => $tmp_dir );

        unless ($ok) {
            my $message
                = "\n[SWORD-DEPOSIT] [INTERNAL-ERROR] Failed to unpack zip-file!";
            $repo->log($message);
            EPrints::Apache::AnApache::send_status_line( $repo->{"request"},
                500, $message );
            return;
        } ## end unless ($ok)
        my $files = $ae->files;

        unless ( defined $files ) {
            my $message = "\n[ERROR] failed to unpack the files";
            $repo->log($message);
            EPrints::Apache::AnApache::send_status_line( $repo->{"request"},
                500, $message );
            return;
        } ## end unless ( defined $files )

        # get the directory with the unpacked files in it.
        my $dir = $ae->extract_path;
        my @candidates = grep ( /^mets.xml$/, @{$files} );

        if ( scalar(@candidates) == 0 ) {
            my $message = "\n[ERROR] could not find the XML file!";
            $repo->log($message);
            EPrints::Apache::AnApache::send_status_line( $repo->{"request"},
                400, $message );
            return;
        } ## end if ( scalar(@candidates...))

        my $file = $candidates[0];

        # $dir is the temporary directory
        # $file is the name of the mets.xml file in that directory

        my $dataset_id   = $opts{dataset_id};
        my $owner_id     = $opts{owner_id};
        my $depositor_id = $opts{depositor_id};

        my $fh;
        if ( !open( $fh, "$dir/$file" ) ) {
            my $message
                = "\n[ERROR] couldnt open the file: '$file' because '$!'";
            $repo->log($message);
            EPrints::Apache::AnApache::send_status_line( $repo->{"request"},
                500, $message );
            return;
        } ## end if ( !open( $fh, "$dir/$file"...))

        # Finally, we can read the xml from the file:
        my $xml;
        while ( my $d = <$fh> ) {
            $xml .= $d;
        }
        close $fh;

        # again, barf if the dataset is not provided (though as this is
        # through SWORD, this shouldn't happen
        if ( !defined $dataset ) {
            my $message
                = "\n[INTERNAL ERROR] dataset (object type) not given!";
            $repo->log($message);
            EPrints::Apache::AnApache::send_status_line( $repo->{"request"},
                500, $message );
            return;
        } ## end if ( !defined $dataset)

        # convert the xml text to a LibXML dom-object
        # What EPrints doesn't do is validate, it just checks for
        # well-formed-ness
        # We have it in an eval block in case the xml is not well-formed,
        # as the parser throws an exception if the document is invalid
        my $dom_doc;
        eval { $dom_doc = EPrints::XML::parse_xml_string($xml); };

        # No dom? barf
        if ( $@ || !defined $dom_doc ) {
            my $message = "\n[ERROR] failed to parse the xml: '$@'";
            $repo->log($message);
            EPrints::Apache::AnApache::send_status_line( $repo->{"request"},
                400, $message );
            return;
        } ## end if ( $@ || !defined $dom_doc)

        if ( !defined $dom_doc ) {
            my $message = "\n[ERROR] failed to parse the xml.";
            $repo->log($message);
            EPrints::Apache::AnApache::send_status_line( $repo->{"request"},
                400, $message );
            return;
        } ## end if ( !defined $dom_doc)

        # If we have got here, we can delete the original mets.xml file
        my $files2 = [];
        @{$files2} = grep ( !/^mets.xml$/, @{$files} );

        # If the top element is not mets, barf
        # This is just in case someone sends us a non METS xml file
        my $dom_top = $dom_doc->getDocumentElement;

        if ( lc $dom_top->tagName ne 'mets' ) {
            my $message
                = "\n[ERROR] failed to parse the xml: no <mets> tag found.";
            $repo->log($message);
            EPrints::Apache::AnApache::send_status_line( $repo->{"request"},
                400, $message );
            return;
        } ## end if ( lc $dom_top->tagName...)

        #####
        # Now we dig down to get the xmldata from the METS dmdSec section
        #####

        # METS Headers (ignored)
        my $mets_hdr = ( $dom_top->getElementsByTagName("metsHdr") )[0];

        # METS Descriptive Metadata (main section for us)

        # need to loop on dmdSec:
        my @dmd_sections = $dom_top->getElementsByTagName("dmdSec");
        my $dmd_id;

        my $md_wrap;
        my $found_wrapper = 0;

        # METS can have more than one dmdSec, and we can't assume that what's
        # pushed in conforms to what we want, so we want to find the stuff
        # formally idendtified as containing epdcx
        # (I know it doesn't guarentee anything, but its a start!)
        foreach my $dmd_sec (@dmd_sections) {

            # need to extract xmlData from here
            $md_wrap = ( $dmd_sec->getElementsByTagName("mdWrap") )[0];

            next if ( !defined $md_wrap );

            next
                if (
                !(     lc $md_wrap->getAttribute("MDTYPE") eq 'other'
                    && defined $md_wrap->getAttribute("OTHERMDTYPE")
                    && lc $md_wrap->getAttribute("OTHERMDTYPE") eq 'epdcx'
                )
                );

            $found_wrapper = 1;
            $dmd_id        = $dmd_sec->getAttribute("ID");
            last;
        } ## end foreach my $dmd_sec (@dmd_sections)

        unless ($found_wrapper) {
            my $message
                = "\n[ERROR] failed to parse the xml: could not find epdcx <mdWrap> section.";
            $repo->log($message);
            EPrints::Apache::AnApache::send_status_line( $repo->{"request"},
                400, $message );
            return;
        } ## end unless ($found_wrapper)

        #########
        # Now we get the (hopefully epdcx) xml from the xmlData section
        ########
        my $xml_data = ( $md_wrap->getElementsByTagName("xmlData") )[0];

        if ( !defined $xml_data ) {
            my $message
                = "\n[ERROR] failed to parse the xml: no <xmlData> tag found.";
            $repo->log($message);
            EPrints::Apache::AnApache::send_status_line( $repo->{"request"},
                400, $message );
            return;
        } ## end if ( !defined $xml_data)

        ########
        # and parse it into a hash
        ########
        my $epdata = $plugin->parse_epdcx_xml_data($xml_data);
        return unless ( defined $epdata );

        ###########################################
        # We now want to deal with the associated files (if any).
        # This is not a straight "every file is a document" import: EPrints
        # has this weird thing where a "document" can have multiple files. The
        # Broker deals with this by having defining the structure in
        # <structMap>, and putting each document into a <div>. If there are
        # multiple files, the main file is the first file in the set

        # The first thing to do is create a list of all the files & their IDs

        # File Section which will contain optional info about files to import:
        my %files;

        # we also need to hang onto details about where the importer unpacked
        # stuff.
        # I know it's nothing to do with files in the METS manifest, however
        # this is probably the best place to keep them :)
        $files{unpack_dir} = $dir;

        my $file_counter = 0;
        foreach my $file_sec ( $dom_top->getElementsByTagName("fileSec") ) {
            my $file_grp = ( $file_sec->getElementsByTagName("fileGrp") )[0];

            $file_sec = $file_grp
                if ( defined $file_grp )
                ;    # this is because the <fileGrp> tag is optional

            foreach my $file_div ( $file_sec->getElementsByTagName("file") ) {
                my $file_id = $file_div->getAttribute("ID");
                my $file_loc
                    = ( $file_div->getElementsByTagName("FLocat") )[0];
                if ( defined $file_loc ) {    # yeepee we have a file (maybe)

                    my $fn = $file_loc->getAttribute("href");

                    unless ( defined $fn ) {

                        # to accommodate the gdome XML library:
                        $fn = $file_loc->getAttribute("xlink:href");
                    }

                    next unless ( defined $fn );

                    $files{$file_id} = $fn;    # list files to get locally
                } ## end if ( defined $file_loc)
            } ## end foreach my $file_div ( $file_sec...)
        } ## end foreach my $file_sec ( $dom_top...)

        unless ( scalar keys %files ) {
            $plugin->add_verbose(
                "[WARNING] no <fileSec> tag found: no files will be imported."
            );
        }


        # Having determined what files we have to add to the deposit, we need
        # to create a "map" of what files go where from the <structMap> element.
        #
        # Oh, and a complication: the embargo information is kept in the
        # amdSec, so we need to find any embargo dates.
        # There will be two options: ClosedAccess and RestrictedAccess (RJB
        # doesn't record OpenAccess dated)
        # The system needs to map the resourceId of the epdcx:description to
        # both the rightsAccess and the date
        $xml_data = "";
        my $amdSec = ( $dom_top->getElementsByTagName("amdSec") )[0];
        $xml_data = ( $amdSec->getElementsByTagName("xmlData") )[0];
        $files{'embargo_hash'} = $plugin->parse_rights_data($xml_data)
            if $xml_data;

        my $struct_map = ( $dom_top->getElementsByTagName("structMap") )[0];
        my $struct_div = ( $struct_map->getElementsByTagName("div") )[0];
        $epdata->{documents}
            = $plugin->process_struct_elements( $struct_div, \%files );

        ##########
        #
        # and finally create the eprint object from the dataset
        #
        my $dataobj = $plugin->epdata_to_dataobj( $dataset, $epdata );
        if ( defined $dataobj ) {
            push @ids, $dataobj->get_id;
        }
    }
    return EPrints::List->new(
        dataset => $dataset,
        session => $repo,
        ids     => \@ids
    );
} ## end sub input_fh

####################################################################

=pod

=item $success = $doc->upload_archive( $filehandle, $filename, $archive_format )

Upload the contents of the given archive file. How to deal with the
archive format is configured in SystemSettings.

(In case the over-loading of the word "archive" is getting confusing,
in this context we mean ".zip" or ".tar.gz" archive.)

=cut

#####################################################################

sub upload_archive {
    my ( $self, $fh ) = @_;

    use bytes;

    binmode($fh);

    my $zipfile = File::Temp->new();
    binmode($zipfile);

    my $rc;
    my $lead;
    while ( $rc = sysread( $fh, my $buffer, 4096 ) ) {
        $lead = $buffer if !defined $lead;
        syswrite( $zipfile, $buffer );
    }
    EPrints->abort("Error reading from file handle: $!") if !defined $rc;

    my $type = substr( $lead, 0, 2 ) eq "PK" ? "zip" : "targz";

    return ( $type, $zipfile );
} ## end sub upload_archive

sub parse_rights_data {
    my ( $plugin, $xml ) = @_;

    my $set = ( $xml->getElementsByTagName("descriptionSet") )[0];

    unless ( defined $set ) {
        $plugin->set_status_code(400);
        $plugin->add_verbose("ERROR: no <descriptionSet> tag found.");
        return;
    }

    my $data = {};

    foreach my $desc ( $set->getElementsByTagName("description") ) {

        # This is the ID that ties the description to the structMap record
        my $att = $desc->getAttribute("epdcx:resourceId");

        foreach my $stat ( $desc->getElementsByTagName("statement") ) {
            my ( $field, $value )
                = EPrints::Plugin::Import::RJ_Broker_parser::parse_statement(
                $stat);
            $data->{$att} = {} unless exists $data->{$att};
            $data->{$att}->{$field} = $value;
        }
    }

    return $data;
}

# This is where we actually track & build the stuff that
# adds the files. It also sets any embargo period.
# As we need the embargo details later on, it returns the
# embargo date and the security restriction.
sub process_struct_elements {
    my ( $plugin, $node_ref, $files_hashref ) = @_;

    my $documents = [];

    my $unpack_dir   = $files_hashref->{unpack_dir};
    my $embargo_hash = $files_hashref->{embargo_hash};

    my ( $embargo, $security );
    $security = 'staffonly';

    # Go through all the children
    foreach my $element ( @{ $node_ref->getElementsByTagName("div") } ) {
        my $ID = $element->getAttribute("ID");

        if ( exists $embargo_hash->{$ID} ) {
            $embargo = $embargo_hash->{$ID}->{'date'};
        }
        my @fptrs = $element->getElementsByTagName("fptr");    #childNodes;

        # check to see what element it is
        if ( scalar @fptrs ) {
            my $doc_data = {};
            $doc_data->{files} = [];
            my $main_filename = "";
            foreach my $fptr (@fptrs) {
                my $fileid = $fptr->getAttribute("FILEID");
                my $file   = $files_hashref->{$fileid};

                # find everything after the last slash
                $file =~ m/\/([^\/]+)$/;
                my $filename = $1 if $1;

                my $filepath = "$unpack_dir/$file";

                if ($embargo) {
                    $doc_data->{date_embargo} = $embargo;
                    $doc_data->{security}     = $security;
                }

                # If there's more than one file, RJB sets the "main" file to
                # be the first one in the list.
                if ( scalar @fptrs > 1 ) {
                    $doc_data->{filename} = $filename
                        unless exists $doc_data->{filename};
                }

                open( my $fh, "<", $filepath )
                    or die "Error opening $filename: $!";
                push @{ $doc_data->{files} },
                    {
                    filename => $filename,
                    filesize => -s $filepath,
                    _content => $fh,
                    };
                $plugin->{session}->run_trigger( EPrints::Const::EP_TRIGGER_MEDIA_INFO,
					epdata => $doc_data,
					filename => $filename,
					filepath => $filepath,
					);
            } ## end foreach my $fptr (@fptrs)
            push @{$documents}, $doc_data;
        } ## end if ( scalar @fptrs )

    } ## end foreach my $element ( @{$nodes_ref...})
    return $documents;
} ## end sub process_struct_elements

sub keep_deposited_file {
    return 1;
}

1;

=head1 COPYRIGHT

=for COPYRIGHT BEGIN

Copyright 2013 EDINA.

=for COPYRIGHT END

=for LICENSE BEGIN

This file is part of EPrints L<http://www.eprints.org/>.

EPrints is free software: you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

EPrints is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
License for more details.

You should have received a copy of the GNU Lesser General Public
License along with EPrints.  If not, see L<http://www.gnu.org/licenses/>.

=for LICENSE END

