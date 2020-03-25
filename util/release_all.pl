#!/usr/bin/env perl™

use strict;
use warnings;
use feature 'say';

my @majors = grep {/\w+/} map {/spark\-(\d\.\d\.\d)/;$1 || ""} qx/git branch -l/;

merge_major(@majors);

exit 0;

sub publish_all {
    foreach my $version (@majors) {
        my $sh = <<SHELL
            git checkout spark-$version
            sbt +publishSigned
SHELL
            ;
        say qx{$sh} || die @!;
    }
}
sub merge_major {
    my @versions = @_;

    my @minors = keys %{{map{/(\d.\d)/;$1 => 1} @versions}};
    foreach my $minor (@minors) {
       my ($last_version) =  (sort {$b cmp $a} grep {/\Q$minor/} @versions);
       foreach my $version (grep {!/$last_version/} @versions) {
           my $sh = <<SHELL
               git checkout spark-$version
               git merge spark-$last_version
               git push
SHELL
;
               say qx{$sh} or die @!;
       }
    }
}
