ALTER TABLE public.splits
    ADD COLUMN secondary_time_range_start BIGINT;

ALTER TABLE public.splits
    ADD COLUMN secondary_time_range_end BIGINT;