#!/bin/bash

set -eu
set -o pipefail

export atcat="$( dirname "${BASH_SOURCE[0]}" )/atomic_cat.bash"

psql service=spd -At -c "
  SELECT
    JSONB_BUILD_OBJECT(
      'state_epoch', (
        SELECT (metadata->'market_state'->'epoch')::INTEGER FROM spd.global
      ),
      'active_replicas', ( SELECT JSONB_AGG(o) FROM (
        SELECT
          JSONB_BUILD_OBJECT(
            'piece_cid', p.piece_cid,
            'piece_log2_size', 35,
            'optional_dag_root', NULL,
            'contracts', JSONB_AGG( JSONB_BUILD_OBJECT(
              'provider_id', pd.provider_id,
              'legacy_market_end_epoch', pd.end_epoch,
              'legacy_market_id', pd.deal_id
            ) ORDER BY pd.end_epoch, pd.deal_id)
          ) o
          FROM spd.published_deals pd
          JOIN spd.pieces p USING ( piece_id )
          JOIN spd.clients c
            ON ( pd.client_id = c.client_id AND c.tenant_id = 13 )
        GROUP BY p.piece_cid
      ) s )
    )
" | zstd -q -19 --long | "$atcat" $HOME/spade/misc/nginx/public/daghaus_active_replicas.json.zst
