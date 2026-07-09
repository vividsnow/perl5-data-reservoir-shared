package Data::Reservoir::Shared;
use strict;
use warnings;
our $VERSION = '0.01';
require XSLoader;
XSLoader::load('Data::Reservoir::Shared', $VERSION);

sub CLONE_SKIP { 1 }  # blessed C-pointer handle: never clone into ithreads (double-free)
1;
__END__

=encoding utf-8

=head1 NAME

Data::Reservoir::Shared - shared-memory reservoir sampler (uniform stream sample)

=head1 SYNOPSIS

    use Data::Reservoir::Shared;

    # keep a uniform random sample of 100 items from an unbounded stream
    my $rsv = Data::Reservoir::Shared->new(undef, 100);

    $rsv->add($_) for @stream;      # feed items; each is kept or discarded

    my @sample = $rsv->sample;      # up to 100 items, uniformly sampled
    $rsv->count;                    # how many are held (min of seen, 100)
    $rsv->seen;                     # total items observed

    # share the reservoir across processes via a backing file
    my $shared = Data::Reservoir::Shared->new("/tmp/sample.rsv", 100);

=head1 DESCRIPTION

A B<reservoir sampler> in shared memory: it keeps a B<uniform random sample of
C<k> items> drawn from a stream of unknown or unbounded length, in fixed memory,
using B<Algorithm R>. Feed it items one at a time with C<add>; while fewer than
C<k> items have been seen every item is kept, and after that the i-th item
replaces a uniformly random slot with probability C<k/i>. At any point the C<k>
retained items are a uniform random sample of everything seen so far -- the
standard way to sample a log stream, sample events for telemetry, or take a fair
sample of a data set too large to hold.

Because the reservoir lives in a shared mapping, B<several processes feed and
read one sample>: any process that opens the same backing file, inherits the
anonymous mapping across C<fork>, or reopens a passed memfd contributes to and
reads the same reservoir. The sampling RNG (a xorshift64) lives in the shared
header and is advanced under the write lock, so concurrent producers sample one
consistent reservoir. A write-preferring futex rwlock with dead-process recovery
guards mutation.

Items are stored B<inline>, truncated to C<item_size> bytes (default 256): an
item longer than C<item_size> keeps only its first C<item_size> bytes. Memory is
C<k * (8 + item_size)> bytes for the slots plus a fixed header. Items are handled
by their B<byte> content; wide-character strings (any codepoint above 255) cause
a "Wide character" croak -- encode to bytes first. B<Linux-only>. Requires 64-bit
Perl.

=head1 METHODS

=head2 Constructors

    my $rsv = Data::Reservoir::Shared->new($path, $k, $item_size);
    my $rsv = Data::Reservoir::Shared->new(undef, $k);              # anonymous, item_size 256
    my $rsv = Data::Reservoir::Shared->new_memfd($name, $k, $item_size);
    my $rsv = Data::Reservoir::Shared->new_from_fd($fd);

C<$k> is the reservoir size (the number of items to retain, at least 1).
C<$item_size> is the maximum bytes stored per item (default 256; items are
truncated to it). C<new> and C<new_memfd> croak on a size below 1 or an
out-of-range C<$item_size>. When reopening an existing file or memfd the stored
geometry wins and the caller's arguments are ignored. An optional file B<mode>
may be passed as the last argument to C<new> (e.g. C<0660>) for cross-user
sharing; it defaults to C<0600> (owner-only).

=head2 Sampling

    my $kept  = $rsv->add($item);        # 1 if the item is now in the reservoir, 0 if discarded
    my $n     = $rsv->add_many(\@items);  # number of the batch that ended up retained
    my @items = $rsv->sample;            # current sample (a list, up to k items)
    my $it    = $rsv->get($i);           # the i-th retained item (0-based), or undef
    $rsv->clear;                         # empty the reservoir (seen resets to 0)

C<add> feeds one item and returns B<1 if it is now stored> in the reservoir or
B<0 if it was discarded>. C<add_many> feeds an array reference under a single
write lock, returning how many of the batch are currently retained. C<sample>
returns the retained items as a list (order is not meaningful); C<get> returns a
single retained item by index. C<clear> empties the reservoir and resets the seen
counter.

=head2 Introspection and RNG

    $rsv->count;        # number of items currently held: min(seen, k)
    $rsv->seen;         # total items observed
    $rsv->capacity;     # k
    $rsv->item_size;    # max bytes per item
    $rsv->seed($n);     # set the RNG state (for reproducible sampling in tests)
    $rsv->stats;        # { size, item_size, count, seen, ops, mmap_size }

C<seed> sets the shared RNG state to a fixed value, making subsequent sampling
deterministic -- useful for reproducible tests. By default the RNG is seeded from
process and clock entropy at creation, so different runs draw different samples.

=head2 Lifecycle

    $rsv->path; $rsv->memfd; $rsv->sync; $rsv->unlink;

C<sync> flushes the mapping to its backing store (a no-op for anonymous and memfd
reservoirs); C<unlink> removes the backing file (also callable as
C<< Class->unlink($path) >>); C<path> returns the backing path (C<undef> for
anonymous, memfd, or fd-reopened reservoirs) and C<memfd> the backing descriptor.

=head1 SHARING ACROSS PROCESSES

The reservoir lives in a shared mapping, shared the same three ways as the rest
of the family: a B<backing file>, an B<anonymous mapping inherited across
C<fork>>, or a B<memfd> passed to an unrelated process and reopened with
C<< new_from_fd($fd) >>. Every process's C<add> feeds the one shared reservoir,
and the shared RNG keeps the sampling probabilities consistent across producers.

=head1 SECURITY

Backing files are created with mode C<0600> (owner-only) by default; pass an
explicit octal mode (e.g. C<0660>) as the last argument to C<new> for cross-user
sharing. The file is opened with C<O_NOFOLLOW> and C<O_EXCL>, and the header is
validated on attach. Any process granted write access is trusted not to corrupt
the mapping.

=head1 CRASH SAFETY

Mutation is guarded by a futex-based write-preferring rwlock with PID-encoded
ownership and dead-owner recovery. Each C<add> is a short bounded update, so a
crash leaves the reservoir consistent up to the last completed operation.
B<Limitation>: PID reuse is not detected (very unlikely in practice).

=head1 SEE ALSO

L<Data::HyperLogLog::Shared> (cardinality estimation), L<Data::CountMinSketch::Shared>
(frequency estimation), and the rest of the C<Data::*::Shared> family.

=head1 AUTHOR

vividsnow

=head1 LICENSE

This is free software; you can redistribute it and/or modify it under the same
terms as Perl itself.

=cut
