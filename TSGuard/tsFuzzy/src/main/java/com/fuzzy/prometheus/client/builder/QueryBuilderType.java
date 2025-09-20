package com.fuzzy.prometheus.client.builder;

import com.fuzzy.prometheus.client.builder.pushGateway.PushGatewaySeriesDeleteBuilder;
import com.fuzzy.prometheus.client.builder.pushGateway.PushGatewaySeriesQueryBuilder;

public enum QueryBuilderType {
	RangeQuery{

		@SuppressWarnings("unchecked")
		@Override
		public RangeQueryBuilder newInstance(String prometheusUrl) {
			return new RangeQueryBuilder(prometheusUrl);
		}
		
	},
	InstantQuery{

		@SuppressWarnings("unchecked")
		@Override
		public InstantQueryBuilder newInstance(String prometheusUrl) {
			return new InstantQueryBuilder(prometheusUrl);
		}
		
	},
	SeriesMetadaQuery{

		@SuppressWarnings("unchecked")
		@Override
		public QueryBuilder newInstance(String prometheusUrl) {
			return new SeriesMetaQueryBuilder(prometheusUrl);
		}
		
	},
	PushGatewaySeriesQuery{

		@SuppressWarnings("unchecked")
		@Override
		public QueryBuilder newInstance(String prometheusUrl) {
			return new PushGatewaySeriesQueryBuilder(prometheusUrl);
		}

	},
	PushGatewaySeriesDelete{

		@SuppressWarnings("unchecked")
		@Override
		public QueryBuilder newInstance(String prometheusUrl) {
			return new PushGatewaySeriesDeleteBuilder(prometheusUrl);
		}

	},

	SeriesDelete{
		@SuppressWarnings("unchecked")
		@Override
		public QueryBuilder newInstance(String prometheusUrl) {
			return new SeriesDeleteBuilder(prometheusUrl);
		}
	},

	CleanTombstones{
		@SuppressWarnings("unchecked")
		@Override
		public QueryBuilder newInstance(String prometheusUrl) {
			return new CleanTombstonesBuilder(prometheusUrl);
		}
	},

	LabelMetadaQuery{

		@SuppressWarnings("unchecked")
		@Override
		public QueryBuilder newInstance(String prometheusUrl) {
			return new LabelMetaQueryBuilder(prometheusUrl);
		}
		
	},

	TargetMetadaQuery{

		@SuppressWarnings("unchecked")
		@Override
		public QueryBuilder newInstance(String prometheusUrl) {
			return new TargetMetaQueryBuilder(prometheusUrl);
		}
		
	},

	AlertManagerMetadaQuery{

		@SuppressWarnings("unchecked")
		@Override
		public QueryBuilder newInstance(String prometheusUrl) {
			return new AlertManagerMetaQueryBuilder(prometheusUrl);
		}
		
	},
	
	StatusMetadaQuery{

		@SuppressWarnings("unchecked")
		@Override
		public QueryBuilder newInstance(String prometheusUrl) {
			return new StatusMetaQueryBuilder(prometheusUrl);
		}
		
	};

	
	public abstract <T extends QueryBuilder> T newInstance(String prometheusUrl);
}
