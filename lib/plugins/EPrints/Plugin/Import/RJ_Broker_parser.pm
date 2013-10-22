
=head1 NAME

EPrints::Plugin::Import::RJ_Broker_parser

=head1 DESCRIPTION

The XML Parser part of the RJ Broker importer.

This file should be common to the SWORD 1.3 & SWORD 2.0 importer

=cut

package EPrints::Plugin::Import::RJ_Broker_parser;

use strict;


use XML::LibXML 1.63;
#use Exporter;

our @ISA = qw/ EPrints::Plugin::Import /;

#our @EXPORT_OK = ("parse_epdcx_xml_data");

# This stops the plugin being available to users
$EPrints::Plugin::Import::DISABLE = 1;

#===  CLASS METHOD  ===========================================================
#        CLASS:  RJ_Broker_parser
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
  $self->{name} = "RJ_Broker XML metadata Parser - do not use directly";

  # Who can see it, and whether we show it
  $self->{visible}   = "";

  return $self;
} ## end sub new

#===  CLASS METHOD  ============================================================
#        CLASS:  RJ_Broker_parser
#       METHOD:  parse_epdcx_xml_data
#   PARAMETERS:  $xml_data
#      RETURNS:  a hashref, of the eprint metadata 
#  DESCRIPTION:  This is main parser routine, which takes mets.xml and returns
#                a reference to a hash of the default EPrints metadata fields
#       THROWS:  no exceptions
#     COMMENTS:  none
#     SEE ALSO:  Called by RJ_Broker (SWORD 1.3) and RJ_Broker_2 (SWORD 2.0)
#===============================================================================
sub parse_epdcx_xml_data {
  my ( $plugin, $xml ) = @_;

  my $set = ( $xml->getElementsByTagName("descriptionSet") )[0];

  unless ( defined $set ) {
    return;
  }

  my $epdata         = {};
  my @creators_array = ();

  foreach my $desc ( $set->getElementsByTagName("description") ) {

    my $att = $desc->getAttribute("epdcx:resourceId");

    for ($att) {

      #  title; abstract; identifer (publisherID);
      #  creator; affilitated institution (from authors)
      # Note: we need to pass in the full $set so we can find
      # creator info
      m#^sword-mets-epdcx# && do {
        $plugin->_parse_epdcx( $desc, $epdata, $set );
        last;
      };

      # type; doi; date (yyyy-mm-dd); status; publisher; copyright_holder;
      m#^sword-mets-expr# && do {
        $plugin->_parse_expr( $desc, $epdata );
        last;
      };

      # publication; issn; isbn; volume; issue; pages;
      m#^sword-mets-manif# && do {
        $plugin->_parse_manif( $desc, $epdata, $set );
        last;
      };
    } ## end for ($att)
  } ## end foreach my $desc ( $set->getElementsByTagName...)

  return $epdata;
} ## end sub parse_epdcx_xml_data

# Parser for the "work" level
#  item type; title; abstract; identifer (publisherID);
#  creator; affilitated institution (from authors)
#  Funder (a ';' seperated list); Grant codes (also a ';' seperated list)
# epdata is passed in, therefore the routine modifies the actual hash
sub _parse_epdcx {
  my ( $plugin, $desc, $epdata, $set ) = @_;

  foreach my $stat ( $desc->getElementsByTagName("statement") ) {
    my ( $field, $value ) = _parse_statement($stat);

    for ($field) {

      # Note - for creators, we have two resources of information
      # 1) There is the name string here in the 'work' section
      # 2) The broker also stores an extended record in a seperate
      #    description with a known resourceId... a resourceId that
      #    matches the $attr value returned here.
      m#creator# && do {
        my ( $name, $attr ) = split /\t/, $value;

        my ( $family, $given, $email, $inst );

        # can we get the information from Person description?
        if ($attr) {
          foreach my $d ( $set->getElementsByTagName("description") ) {
            next
                unless $d->getAttribute('epdcx:resourceId') eq $attr;

            foreach my $stat2 ( $d->getElementsByTagName("statement") ) {
              my ( $field2, $value2 ) = _parse_statement($stat2);

              for ($field2) {

                m#givenname#
                    && do { $given = $value2; last; };
                m#familyname#
                    && do { $family = $value2; last; };
                m#email# && do { $email = $value2; last; };
              } ## end for ($field2)
            } ## end foreach my $stat2 ( $d->getElementsByTagName...)
          } ## end foreach my $d ( $set->getElementsByTagName...)
        } ## end if ($attr)

        # if not, deduce it from the text here
        unless ($family) {
          if ( $name =~ /(\w+)\,\s?(.*)$/ ) {
            $family = $1, $given = $2;
          } else {
            $family = $value;
          }
        } ## end unless ($family)

        # now add the information to the eprint
        # NOTE: eprints uses the index into arrays to syncronise
        # names, email addresses and other fields, so we have to
        # add something, even if its NULL
        push @{ $epdata->{creators_name} },
            { family => $family, given => $given };
        push @{ $epdata->{creators_id} }, $email;

        last;
      };

      m#title# && do {
        $epdata->{'title'} = $value;
        last;
      };

      m#abstract# && do {
        $epdata->{'abstract'} = $value;
        last;
      };

      m#identifier# && do {
        $epdata->{'id_number'} = $value;
        last;
      };

      # In EPrints, there is a "Funders" field, but no grant.
      # This is a repeating field, so we can push the items onto an array
      # To note grant codes, we are bodging the string:
      # <funder>: <grant>, <grant>, <grant>
      # ... it makes it human readable, and computer-parseable
      m#funders# && do {

        if ($value) {
          my ( $funder, $gcode );
          $funder = $value;
          $funder =~ s/^funder //;
          my @grants = ();
          foreach my $d ( $set->getElementsByTagName("description") ) {
            next
                unless $d->getAttribute('epdcx:resourceId') eq $value;

            foreach my $stat2 ( $d->getElementsByTagName("statement") ) {
              my ( $field2, $value2 ) = _parse_statement($stat2);
              if ( $field2 eq 'grant_code' ) {
                push @grants, $value2;
              }
            } ## end foreach my $stat2 ( $d->getElementsByTagName...)
            if ( scalar @grants ) {
              $funder .= ': ';
              $funder .= join '; ', @grants;
            }
          } ## end foreach my $d ( $set->getElementsByTagName...)
          push @{ $epdata->{'funders'} }, $funder;
        } ## end if ($value)
        last;
      };

      # This becomes a pseudo-list (as EPrints doesn't have this
      # as a multiple field
      m#affiliatedInstitution# && do {
        my $i;
        $i = $epdata->{'institution'}
            if exists $epdata->{'institution'};
        if ($i) {
          my @i = split /; /, $i;
          push @i, $value;
          $value = join '; ', @i;
        }
        $epdata->{'institution'} = $value;
        last;
      };
    } ## end for ($field)

  } ## end foreach my $stat ( $desc->getElementsByTagName...)

} ## end sub _parse_epdcx

# Parser for the "expression" level
# genre, citation, doi, status, publisher, editor (provenance for
# opendepot), date published
# epdata is passed in, therefore the routine modifies the actual hash
sub _parse_expr {
  my ( $plugin, $desc, $epdata ) = @_;
  foreach my $stat ( $desc->getElementsByTagName("statement") ) {
    my ( $field, $value ) = _parse_statement($stat);

    for ($field) {

      # This gets used once we have built the full record...
      m#type# && do {
        $epdata->{'type'} = $value;
        last;
      };

      # date (published)
      m#date# && do {
        $epdata->{ispublished} = 'pub';
        $epdata->{date}        = $value;
        $epdata->{date_type}   = 'published';
        last;
      };

      # This could be either a local identifier
      # a local identifier code, or a URI into the broker
      # We definitely don't want the latter.
      # Of the first two, take a doi in preference to a simple identifier
      m#identifier# && do {
        last
            if ( $value =~ /edina\.ac\.uk/ )
            ;    # skip anything that refers to edina
        my $i;
        $i = $epdata->{identifier} if exists $epdata->{identifier};
        last if ( $i =~ /doi\.org/ );    # skip if we already have a doi
        $epdata->{identifier} = $value;
        last;
      };

      # peer reviewed
      m#refereed# && do {
        $epdata->{'refereed'} = $value;
        last;
      };

      # we need to come back to this: for a thesis, the broker
      # understands that thesis are "institution; department"
      # whereas everything else is just "publisher"
      # stuff it in publisher for now, and sort it later
      m#publisher# && do {
        $epdata->{'publisher'} = $value;
        last;
      };

      m#provenance# && do {
        $epdata->{'provenance'} = $value;    # copyright_holder?
        last;
      };

    } ## end for ($field)

  } ## end foreach my $stat ( $desc->getElementsByTagName...)

} ## end sub _parse_expr

# Parser for the "manifest" level
# epdata is passed in, therefore the routine modifies the actual hash
sub _parse_manif {
  my ( $plugin, $desc, $epdata, $set ) = @_;
  foreach my $stat ( $desc->getElementsByTagName("statement") ) {
    my ( $field, $value ) = _parse_statement($stat);

    for ($field) {

      m#cite# && do {

        my $cite = clean_text($value);

        # now we need to decode the citation
        # $publication [$vol[($num)]][, $pps|$pgs]
        my ( $pub, $vol, $num, $pps, $pgs );

        # Start by pulling the page range or number of pages off the end
        if ( $cite =~ /\, (\d+(:?\-\d+))$/ ) {
          $cite =~ s/\, (\d+(:?\-\d+))$//;
          $num = $1 if $1;
          if   ( $num =~ /\-/ ) { $pps = $num }
          else                  { $pgs = $num }
          $num = undef;
        } ## end if ( $cite =~ /\, (\d+(:?\-\d+))$/)

        # Do we have volume number ?
        if ( $cite =~ /\((.+)\)$/ ) {
          $cite =~ s/\((.+)\)$//;
          $num = $1 if $1;
        }

        # Get the volume number
        if ( $cite =~ /\s+(\d+)$/ ) {
          $cite =~ s/\s+(\d+)$//;
          $vol = $1 if $1;
        }

        $pub = $cite;
        $epdata->{publication} = $pub
            if ( $pub && not exists $epdata->{publication} );
        $epdata->{volume} = $vol
            if ( $vol && not exists $epdata->{volume} );
        $epdata->{number} = $num
            if ( $num && not exists $epdata->{number} );
        $epdata->{pagerange} = $pps
            if ( $pps && not exists $epdata->{pagerange} );

        #          $epdata->{pages}       = $pgs if $pgs;
        last;
      };

      # issn & isbn
      m#issn# && do {
        $epdata->{'issn'} = $value;
        last;
      };
      m#isbn# && do {
        $epdata->{'isbn'} = $value;
        last;
      };

      # publication,volume, issue, pages
      # these trump anything from the citation
      m#publication# && do {
        $epdata->{'publication'} = $value;
        last;
      };
      m#volume# && do {
        $epdata->{'volume'} = $value;
        last;
      };
      m#issue# && do {
        $epdata->{'issue'} = $value;
        last;
      };
      m#pagerange# && do {
        $epdata->{'pagerange'} = $value;
        last;
      };
      m#pages# && do {

        # Pages has to be a numeric value!
        $epdata->{'pages'} = $value + 0;
        last;
      };

      # Available URLS (DOI, official_url, related urls)
      m#available_url# && do {

     # available-doi-1; available-official_url-1; available-related_url-4; ...
     # can we get the information from Person description?
        if ($value) {
          foreach my $d ( $set->getElementsByTagName("description") ) {
            next
                unless $d->getAttribute('epdcx:resourceId') eq $value;

            my $uri;
            $uri = $d->getAttribute('epdcx:resourceUrl');
            if ( $value =~ /official/ ) {
              $epdata->{'official_url'} = $uri;
            } elsif ( $value =~ /doi/ ) {
              $epdata->{'id_number'} = $uri;
            } else {
              push @{ $epdata->{'related_url'} },
                  { url => $uri, type => 'pub' }
                  if $uri;
            }
          } ## end foreach my $d ( $set->getElementsByTagName...)
        } ## end if ($value)

        last;
      };
    } ## end for ($field)

  } ## end foreach my $stat ( $desc->getElementsByTagName...)

} ## end sub _parse_manif

# Parser for the unknown level
# This is where we try to find the author/creator information
# epdata is passed in, therefore the routine modifies the actual hash
sub _parse_unknown {
  my ( $plugin, $desc, $epdata ) = @_;
  $epdata->{unknown} = {} unless exists $epdata->{unknown};
  my $resourceId;
  $resourceId = $desc->getAttribute("resourceId");

  # we only parse if we have a resourceId: creators have them
  if ($resourceId) {
    $epdata->{unknown}->{$resourceId} = {};
    foreach my $stat ( $desc->getElementsByTagName("statement") ) {
      my ( $field, $value ) = _parse_statement($stat);

      for ($field) {
        m#givenname# && do {
          $epdata->{'pages'} = $value;
          last;
        };

        #familyname
        #institution
        #email
      } ## end for ($field)
    } ## end foreach my $stat ( $desc->getElementsByTagName...)
  } ## end if ($resourceId)
} ## end sub _parse_unknown

sub _parse_statement {
  my ($stat) = @_;

  my $property = $stat->getAttribute("propertyURI");

  unless ( defined $property ) {
    $property = $stat->getAttribute("epdcx:propertyURI");
  }

  for ($property) {
    m#http://purl.org/dc/elements/1.1/type# && do {
      return _get_type($stat);
      last;
    };    ## end if ($property eq 'http://purl.org/dc/elements/1.1/type'...)

    m#http://purl.org/dc/elements/1.1/title# && do {
      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      my $title
          = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
      return ( "title", clean_text($title) );
      last;
    };  ## end elsif ($property eq 'http://purl.org/dc/elements/1.1/title'...)

    m#http://purl.org/dc/terms/abstract# && do {
      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      my $abstract
          = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
      return ( "abstract", clean_text($abstract) );
      last;
    };    ## end elsif ($property eq 'http://purl.org/dc/terms/abstract'...)

    m#http://purl.org/dc/elements/1.1/creator# && do {

      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      $value .= "\t"
          . $stat->getAttribute("epdcx:valueRef")
          ;    # add the value of the attribute to the creators list

      return ( "creator", $value );
      last;
    }; ## end elsif ($property eq 'http://purl.org/dc/elements/1.1/creator'...)

    m#http://purl.org/dc/elements/1.1/identifier# && do {
      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      my $id = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
      return ( "id_number", clean_text($id) );
      last;
    }; ## end elsif ($property eq 'http://purl.org/dc/elements/1.1/identifier'...)

    m#http://purl.org/eprint/terms/status# && do {
      if ( $stat->getAttribute("valueURI") =~ /status\/(.*)$/ ) {
        my $status = $1;
        if ( $status eq 'PeerReviewed' ) {
          $status = 'TRUE';
        } elsif ( $status eq 'NonPeerReviewed' ) {
          $status = 'FALSE';
        } else {
          return;
        }

        return ( 'refereed', $status );    # is this the proper field?

      } ## end if ( $stat->getAttribute...)

      return;
      last;
    };    ## end elsif ($property eq 'http://purl.org/eprint/terms/status'...)

    m#http://purl.org/dc/elements/1.1/language# && do {

      # LANGUAGE (not parsed)
      last;
    };

    m#http://purl.org/dc/terms/available# && do {
      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      my $id = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
      return ( "date", clean_text($id) );
      last;
    };

    m#http://purl.org/eprint/terms/copyrightHolder# && do {

      # COPYRIGHT HOLDER (not parsed)
      last;
    };

    m#http://purl.org/dc/terms/bibliographicCitation# && do {
      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      my $cite = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
      return ( 'cite', $cite );
      last;
    };

    m#http://purl.org/dc/elements/1.1/publisher# && do {
      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      $value = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
      return ( 'publisher', $value );
      last;
    };

    # This lot are OA-RJ extensions to cover blantently missing fields
    m#http://opendepot.org/broker/elements/1.0/publication# && do {
      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      $value = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
      return ( 'publication', $value );
      last;
    };

    m#http://opendepot.org/broker/elements/1.0/issn# && do {
      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      $value = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
      return ( 'issn', $value );
      last;
    };

    m#http://opendepot.org/broker/elements/1.0/isbn# && do {
      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      $value = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
      return ( 'isbn', $value );
      last;
    };

    m#http://opendepot.org/broker/elements/1.0/volume# && do {
      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      $value = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
      return ( 'volume', $value );
      last;
    };

    m#http://opendepot.org/broker/elements/1.0/issue# && do {
      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      $value = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
      return ( 'issue', $value );
      last;
    };

    m#http://opendepot.org/broker/elements/1.0/pages# && do {
      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      $value = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
      return ( 'pagerange', $value ) if $value =~ /\-/;
      return ( 'pages', $value );
      last;
    };

## new record metadata
    m#http://purl.org/dc/elements/1.1/givenname# && do {
      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      $value = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
      return ( 'givenname', $value );
      last;
    };

    m#http://purl.org/dc/elements/1.1/familyname# && do {
      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      $value = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
      return ( 'familyname', $value );
      last;
    };

    m#http://xmlns.com/foaf/0.1/mbox# && do {
      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      $value = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
      return ( 'email', $value );
      last;
    };

    m#http://purl.org/eprint/terms/affiliatedInstitution# && do {
      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      $value = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
      return ( 'institution', $value );
      last;
    };

    m#http://www.loc.gov/loc.terms/relators/EDT# && do {
      my $value = ( $stat->getElementsByTagName("valueString") )[0];
      $value = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
      return ( 'provenance', $value );    # editor?
      last;
    };

    m#http://www.loc.gov/loc.terms/relators/FND# && do {
      my $value = $stat->getAttribute("epdcx:valueRef")
          ;    # add the value of the attribute to the creators list
      return ( "funders", $value );

      last;
    };
    m#http://purl.org/eprint/terms/grantNumber# && do {
      my $value;

      if ( $stat->hasAttribute("epdcx:valueRef") ) {
        $value = $stat->getAttribute("epdcx:valueRef");
      } else {
        my @vs = $stat->getElementsByTagName("valueString");
        if ( scalar @vs ) {
          $value = $vs[0];
          $value
              = EPrints::XML::to_string( EPrints::XML::contents_of($value) );
        }
      } ## end else [ if ( $stat->hasAttribute...)]
      return ( "grant_code", $value );
      last;
    };

## end new data

## Alternate sources
    m#http://purl.org/eprint/terms/isAvailableAs# && do {
      my $value = $stat->getAttribute("epdcx:valueRef");
      return ( "available_url", $value );
      last;
    };

## Data for embargoes
    m#http://purl.org/dc/terms/accessRights# && do {
      my $value = $stat->getAttribute("epdcx:valueRef")
          ;    # add the value of the attribute to the creators list
      $value =~ /([^\/]+)$/;
      $value = $1 if $1;
      return ( "access", $value );

      last;
    };
  } ## end for ($property)
  return;

} ## end sub _parse_statement

sub _get_type {
  my ($stat) = @_;

  if ( $stat->getAttribute("valueURI") =~ /type\/(.*)$/ ) {   # then $1 = type

# reference for these mappings is:
# "http://www.ukoln.ac.uk/repositories/digirep/index/Eprints_Type_Vocabulary_Encoding_Scheme"

    my $type = $1;    # no need to clean_text( $1 ) here

    if ( $type eq 'JournalArticle'
      || $type eq 'JournalItem'
      || $type eq 'SubmittedJournalArticle'
      || $type eq 'WorkingPaper' )
    {
      $type = 'article';
    } ## end if ( $type eq 'JournalArticle'...)
    elsif ( $type eq 'Book' ) {
      $type = 'book';
    } elsif ( $type eq 'BookItem' ) {
      $type = 'book_section';
    } elsif ( $type eq 'ConferenceItem'
      || $type eq 'ConferencePoster'
      || $type eq 'ConferencePaper' )
    {
      $type = 'conference_item';
    } ## end elsif ( $type eq 'ConferenceItem'...)
    elsif ( $type eq 'Patent' ) {
      $type = 'patent';
    } elsif ( $type eq 'Report' ) {
      $type = 'monograph';    # I think?
    } elsif ( $type eq 'Thesis' ) {
      $type = 'thesis';
    } else {
      return
          ; # problem there! But the user can still correct this piece of data...
    }

    return ( "type", $type );
  } ## end if ( $stat->getAttribute...)
  return;
} ## end sub _get_type

sub clean_text {
  my ($text) = @_;

  my @lines = split( "\n", $text );

  foreach (@lines) {
    $_ =~ s/\s+$//;

    $_ =~ s/^\s+//;
  }

  return join( " ", @lines );
} ## end sub clean_text

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

