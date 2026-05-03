import React, { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Container,
  CssBaseline,
  Divider,
  Grid,
  LinearProgress,
  Link,
  Paper,
  Stack,
  ThemeProvider,
  Typography,
  createTheme,
} from '@mui/material';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import DatasetIcon from '@mui/icons-material/Dataset';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';
import FlashOnIcon from '@mui/icons-material/FlashOn';
import HealthAndSafetyIcon from '@mui/icons-material/HealthAndSafety';
import ReportProblemIcon from '@mui/icons-material/ReportProblem';
import SpeedIcon from '@mui/icons-material/Speed';
import TimelineIcon from '@mui/icons-material/Timeline';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

const theme = createTheme({
  palette: {
    mode: 'light',
    primary: { main: '#00796b', contrastText: '#ffffff' },
    secondary: { main: '#bd3f53', contrastText: '#ffffff' },
    success: { main: '#25824f' },
    warning: { main: '#ad7c00' },
    error: { main: '#b3263d' },
    background: {
      default: '#f4f8f6',
      paper: '#ffffff',
    },
    text: {
      primary: '#17211f',
      secondary: '#51625d',
    },
  },
  typography: {
    fontFamily:
      'Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
    h1: { fontWeight: 800, letterSpacing: 0 },
    h2: { fontWeight: 800, letterSpacing: 0 },
    h3: { fontWeight: 800, letterSpacing: 0 },
    h4: { fontWeight: 800, letterSpacing: 0 },
    h5: { fontWeight: 800, letterSpacing: 0 },
    h6: { fontWeight: 800, letterSpacing: 0 },
    button: { fontWeight: 800, letterSpacing: 0, textTransform: 'none' },
  },
  shape: {
    borderRadius: 8,
  },
  components: {
    MuiPaper: {
      styleOverrides: {
        root: {
          backgroundImage: 'none',
        },
      },
    },
  },
});

const chartColors = ['#00796b', '#bd3f53', '#ad7c00', '#25824f', '#435b57', '#6f4e37'];

const topology = [
  { label: 'Synthetic EHR', detail: 'vitals, labs, medication, admission' },
  { label: 'Kafka', detail: 'ehr.raw, valid, quarantine, alerts' },
  { label: 'Flink', detail: 'validation, watermarks, keyed state' },
  { label: 'Redis', detail: 'patient metadata enrichment' },
  { label: 'Cassandra', detail: 'valid records, quarantine, alerts' },
];

function App() {
  const [summary, setSummary] = useState(null);
  const [error, setError] = useState('');

  useEffect(() => {
    async function loadSummary() {
      try {
        const response = await fetch(`/distributed_summary.json?ts=${Date.now()}`);
        if (!response.ok) {
          throw new Error('current summary not found');
        }
        setSummary(await response.json());
      } catch (firstError) {
        try {
          const fallback = await fetch('/distributed_summary.example.json');
          setSummary(await fallback.json());
          setError('Showing bundled example data. Run the pipeline to mount the latest summary.');
        } catch (fallbackError) {
          setError(`Could not load dashboard data: ${fallbackError.message}`);
        }
      }
    }
    loadSummary();
  }, []);

  if (!summary) {
    return (
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <CenterStage>
          {error ? <Alert severity="error">{error}</Alert> : <CircularProgress />}
        </CenterStage>
      </ThemeProvider>
    );
  }

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      {isPreliminaryView() ? (
        <PreliminaryDashboard summary={summary} warning={error} />
      ) : (
        <Dashboard summary={summary} warning={error} />
      )}
    </ThemeProvider>
  );
}

function isPreliminaryView() {
  const params = new URLSearchParams(window.location.search);
  return params.get('view') === 'preliminary' || params.has('preliminary');
}

function Dashboard({ summary, warning }) {
  const view = useMemo(() => normalizeSummary(summary), [summary]);

  return (
    <Box sx={{ minHeight: '100vh', pb: 6 }}>
      <Box
        component="header"
        sx={{
          borderBottom: '1px solid',
          borderColor: 'divider',
          bgcolor: 'background.paper',
        }}
      >
        <Container maxWidth="xl" sx={{ py: { xs: 2, md: 3 } }}>
          <Stack
            direction={{ xs: 'column', md: 'row' }}
            justifyContent="space-between"
            alignItems={{ xs: 'flex-start', md: 'center' }}
            gap={2}
          >
            <Box>
              <Stack direction="row" gap={1} flexWrap="wrap" sx={{ mb: 1 }}>
                <Chip size="small" color="primary" label="Kafka" />
                <Chip size="small" color="primary" label="Flink" />
                <Chip size="small" color="primary" label="Redis" />
                <Chip size="small" color="primary" label="Cassandra" />
              </Stack>
              <Typography variant="h3" component="h1" sx={{ fontSize: { xs: 30, md: 44 } }}>
                Streaming EHR Ingestion Dashboard
              </Typography>
              <Typography color="text.secondary" sx={{ mt: 1, maxWidth: 850, lineHeight: 1.55 }}>
                Local synthetic EHR stream processed through Kafka, Flink, Redis, and Cassandra.
                This view is driven by the generated pipeline summary JSON.
              </Typography>
            </Box>
            <Stack direction={{ xs: 'column', sm: 'row' }} gap={1} sx={{ width: { xs: '100%', md: 'auto' } }}>
              <Button
                component={Link}
                href="http://localhost:8081"
                target="_blank"
                rel="noreferrer"
                variant="outlined"
                color="primary"
              >
                Open Flink
              </Button>
              <Button
                variant="contained"
                color="secondary"
                onClick={() => navigator.clipboard?.writeText('EVENTS=5000 PATIENTS=1000 RATE=0 ALLOWED_LATENESS=300 ./scripts/run_distributed_pipeline.sh')}
              >
                Copy Run Command
              </Button>
            </Stack>
          </Stack>
        </Container>
      </Box>

      <Container maxWidth="xl" sx={{ pt: 3 }}>
        {warning && (
          <Alert severity="warning" sx={{ mb: 2 }}>
            {warning}
          </Alert>
        )}

        <Grid container spacing={2} sx={{ mb: 2 }}>
          <MetricCard
            title="Valid Records"
            value={formatNumber(view.totals.valid)}
            subtitle={`${view.totals.acceptedPercent.toFixed(2)}% accepted`}
            icon={<CheckCircleIcon />}
            color="#00796b"
          />
          <MetricCard
            title="Quarantined"
            value={formatNumber(view.totals.quarantine)}
            subtitle="invalid, duplicate, or malformed"
            icon={<ReportProblemIcon />}
            color="#bd3f53"
          />
          <MetricCard
            title="Alerts"
            value={formatNumber(view.totals.alerts)}
            subtitle="possible sepsis rule hits"
            icon={<HealthAndSafetyIcon />}
            color="#ad7c00"
          />
          <MetricCard
            title="Throughput"
            value={`${formatNumber(Math.round(view.producer.throughput))}/s`}
            subtitle={`${formatNumber(view.producer.sent)} events sent`}
            icon={<SpeedIcon />}
            color="#25824f"
          />
          <MetricCard
            title="P95 Latency"
            value={`${formatNumber(Math.round(view.latency.p95))} ms`}
            subtitle={`avg ${formatNumber(Math.round(view.latency.avg))} ms`}
            icon={<TimelineIcon />}
            color="#435b57"
          />
          <MetricCard
            title="Patients"
            value={formatNumber(view.producer.patients)}
            subtitle={`lateness ${view.producer.allowedLateness}s`}
            icon={<DatasetIcon />}
            color="#7a5a00"
          />
        </Grid>

        <Grid container spacing={2}>
          <Grid item xs={12} lg={5}>
            <Panel title="Valid Vs Quarantined" action={`${formatNumber(view.totals.processed)} processed`}>
              <Box sx={{ height: 320 }}>
                <ResponsiveContainer>
                  <PieChart>
                    <Pie
                      data={view.validityData}
                      dataKey="value"
                      nameKey="name"
                      innerRadius={78}
                      outerRadius={118}
                      paddingAngle={3}
                    >
                      {view.validityData.map((entry, index) => (
                        <Cell key={entry.name} fill={chartColors[index % chartColors.length]} />
                      ))}
                    </Pie>
                    <Tooltip formatter={(value) => formatNumber(value)} />
                    <Legend />
                  </PieChart>
                </ResponsiveContainer>
              </Box>
            </Panel>
          </Grid>

          <Grid item xs={12} lg={7}>
            <Panel title="Throughput And Latency" action="producer + Cassandra summary">
              <Box sx={{ height: 320 }}>
                <ResponsiveContainer>
                  <BarChart data={view.performanceData} margin={{ top: 10, right: 20, left: 8, bottom: 18 }}>
                    <CartesianGrid strokeDasharray="4 4" stroke="#d8e3df" />
                    <XAxis dataKey="name" interval={0} tick={{ fontSize: 12 }} />
                    <YAxis tickFormatter={(value) => formatCompact(value)} width={74} />
                    <Tooltip formatter={(value) => formatNumber(Math.round(value))} />
                    <Bar dataKey="value" radius={[6, 6, 0, 0]}>
                      {view.performanceData.map((entry, index) => (
                        <Cell key={entry.name} fill={chartColors[index % chartColors.length]} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </Box>
            </Panel>
          </Grid>

          <Grid item xs={12} lg={7}>
            <Panel title="Events Stored By Type" action="Cassandra events_by_type">
              <Box sx={{ height: 330 }}>
                <ResponsiveContainer>
                  <BarChart data={view.eventData} margin={{ top: 10, right: 20, left: 8, bottom: 18 }}>
                    <CartesianGrid strokeDasharray="4 4" stroke="#d8e3df" />
                    <XAxis dataKey="name" tick={{ fontSize: 12 }} />
                    <YAxis tickFormatter={(value) => formatCompact(value)} width={72} />
                    <Tooltip formatter={(value) => formatNumber(value)} />
                    <Bar dataKey="value" fill="#00796b" radius={[6, 6, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </Box>
            </Panel>
          </Grid>

          <Grid item xs={12} lg={5}>
            <Panel title="Quarantine Reasons" action="validation rejects">
              <Stack gap={1.1}>
                {view.reasonData.map((item) => (
                  <ReasonRow key={item.name} item={item} max={view.maxReason} />
                ))}
              </Stack>
            </Panel>
          </Grid>

          <Grid item xs={12} lg={8}>
            <Panel title="Pipeline Topology" action="local Docker network">
              <Grid container spacing={1.5}>
                {topology.map((step, index) => (
                  <Grid item xs={12} sm={6} md key={step.label}>
                    <Box
                      sx={{
                        border: '1px solid',
                        borderColor: 'divider',
                        borderRadius: 2,
                        p: 1.5,
                        minHeight: 104,
                        bgcolor: index % 2 === 0 ? '#f8fbfa' : '#fff9f6',
                      }}
                    >
                      <Typography variant="overline" color="text.secondary">
                        Step {index + 1}
                      </Typography>
                      <Typography variant="h6">{step.label}</Typography>
                      <Typography color="text.secondary" sx={{ fontSize: 14, lineHeight: 1.35 }}>
                        {step.detail}
                      </Typography>
                    </Box>
                  </Grid>
                ))}
              </Grid>
            </Panel>
          </Grid>

          <Grid item xs={12} lg={4}>
            <Panel title="Run Profile" action={`seed ${view.producer.seed}`}>
              <Stack gap={1.4}>
                <ProfileLine label="Invalid injection" value={`${percent(view.producer.invalidRate)}%`} />
                <ProfileLine label="Duplicate injection" value={`${percent(view.producer.duplicateRate)}%`} />
                <ProfileLine label="Out-of-order injection" value={`${percent(view.producer.outOfOrderRate)}%`} />
                <ProfileLine label="Late-event injection" value={`${percent(view.producer.lateRate)}%`} />
                <Divider />
                <Typography color="text.secondary" sx={{ fontSize: 14 }}>
                  Full infrastructure runs locally in Docker. No real patient data or cloud service is used.
                </Typography>
              </Stack>
            </Panel>
          </Grid>
        </Grid>
      </Container>
    </Box>
  );
}

function PreliminaryDashboard({ summary, warning }) {
  const view = useMemo(() => normalizeSummary(summary), [summary]);

  return (
    <Box sx={{ minHeight: '100vh', pb: 6 }}>
      <Box
        component="header"
        sx={{
          borderBottom: '1px solid',
          borderColor: 'divider',
          bgcolor: 'background.paper',
        }}
      >
        <Container maxWidth="xl" sx={{ py: { xs: 3, md: 4 } }}>
          <Stack direction="row" gap={1} flexWrap="wrap" sx={{ mb: 1.2 }}>
            <Chip size="small" color="secondary" label="Preliminary View" />
            <Chip size="small" color="primary" label="Preliminary Pipeline Run" />
          </Stack>
          <Typography variant="h3" component="h1" sx={{ fontSize: { xs: 30, md: 44 } }}>
            Preliminary Streaming EHR Pipeline Output
          </Typography>
          <Typography color="text.secondary" sx={{ mt: 1, maxWidth: 860, lineHeight: 1.55 }}>
            This view shows a small end-to-end preliminary run with real local Kafka,
            Flink, Redis, and Cassandra containers. Larger workload experiments
            are planned for the final evaluation.
          </Typography>
          <Stack direction={{ xs: 'column', sm: 'row' }} gap={1} sx={{ mt: 2 }}>
            <Button
              component={Link}
              href="/"
              variant="outlined"
              color="primary"
            >
              Open Full Dashboard
            </Button>
            <Button
              component={Link}
              href="http://localhost:8081"
              target="_blank"
              rel="noreferrer"
              variant="contained"
              color="secondary"
            >
              Open Flink
            </Button>
          </Stack>
        </Container>
      </Box>

      <Container maxWidth="xl" sx={{ pt: 3 }}>
        {warning && (
          <Alert severity="warning" sx={{ mb: 2 }}>
            {warning}
          </Alert>
        )}
        <Alert severity="info" sx={{ mb: 2 }}>
          These are preliminary results from a small local pipeline run. Larger workloads,
          event-rate comparisons, and final analysis will be completed for the final presentation.
        </Alert>

        <Grid container spacing={2} sx={{ mb: 2 }}>
          <MetricCard
            title="Pipeline Events"
            value={formatNumber(view.producer.sent)}
            subtitle="synthetic EHR stream"
            icon={<FlashOnIcon />}
            color="#25824f"
          />
          <MetricCard
            title="Accepted Records"
            value={formatNumber(view.totals.valid)}
            subtitle={`${view.totals.acceptedPercent.toFixed(1)}% accepted`}
            icon={<CheckCircleIcon />}
            color="#00796b"
          />
          <MetricCard
            title="Quarantined"
            value={formatNumber(view.totals.quarantine)}
            subtitle="validation path tested"
            icon={<ReportProblemIcon />}
            color="#bd3f53"
          />
          <MetricCard
            title="Prelim Throughput"
            value={`${formatNumber(Math.round(view.producer.throughput))}/s`}
            subtitle="single laptop run"
            icon={<SpeedIcon />}
            color="#435b57"
          />
        </Grid>

        <Grid container spacing={2}>
          <Grid item xs={12} lg={5}>
            <Panel title="Preliminary Validity Split" action={`${formatNumber(view.totals.processed)} processed`}>
              <Box sx={{ height: 300 }}>
                <ResponsiveContainer>
                  <PieChart>
                    <Pie
                      data={view.validityData}
                      dataKey="value"
                      nameKey="name"
                      innerRadius={72}
                      outerRadius={112}
                      paddingAngle={3}
                    >
                      {view.validityData.map((entry, index) => (
                        <Cell key={entry.name} fill={chartColors[index % chartColors.length]} />
                      ))}
                    </Pie>
                    <Tooltip formatter={(value) => formatNumber(value)} />
                    <Legend />
                  </PieChart>
                </ResponsiveContainer>
              </Box>
            </Panel>
          </Grid>

          <Grid item xs={12} lg={7}>
            <Panel title="Events Stored By Type" action="Cassandra output">
              <Box sx={{ height: 300 }}>
                <ResponsiveContainer>
                  <BarChart data={view.eventData} margin={{ top: 10, right: 20, left: 8, bottom: 18 }}>
                    <CartesianGrid strokeDasharray="4 4" stroke="#d8e3df" />
                    <XAxis dataKey="name" tick={{ fontSize: 12 }} />
                    <YAxis tickFormatter={(value) => formatCompact(value)} width={72} />
                    <Tooltip formatter={(value) => formatNumber(value)} />
                    <Bar dataKey="value" fill="#00796b" radius={[6, 6, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </Box>
            </Panel>
          </Grid>

          <Grid item xs={12} lg={7}>
            <Panel title="Implemented Prototype Pipeline" action="real local stack">
              <Grid container spacing={1.5}>
                {topology.map((step, index) => (
                  <Grid item xs={12} sm={index === 0 ? 12 : 6} key={step.label}>
                    <Box
                      sx={{
                        border: '1px solid',
                        borderColor: 'divider',
                        borderRadius: 2,
                        p: 1.5,
                        bgcolor: index % 2 === 0 ? '#f8fbfa' : '#fff9f6',
                      }}
                    >
                      <Typography variant="overline" color="text.secondary">
                        Step {index + 1}
                      </Typography>
                      <Typography variant="h6">{step.label}</Typography>
                      <Typography color="text.secondary" sx={{ fontSize: 14 }}>
                        {step.detail}
                      </Typography>
                    </Box>
                  </Grid>
                ))}
              </Grid>
            </Panel>
          </Grid>

          <Grid item xs={12} lg={5}>
            <Panel title="What This Preliminary Output Shows" action="preliminary evidence">
              <Stack gap={1.4}>
                <ProfileLine label="Kafka ingestion path" value="working" />
                <ProfileLine label="Flink validation job" value="running" />
                <ProfileLine label="Redis enrichment layer" value="connected" />
                <ProfileLine label="Cassandra storage output" value="generated" />
                <ProfileLine label="React dashboard summary" value="generated" />
                <Divider />
                <Typography color="text.secondary" sx={{ lineHeight: 1.5 }}>
                  For the final report, this preliminary run will be extended into a controlled
                  experiment matrix with larger event counts, multiple producer rates,
                  lateness-window settings, latency analysis, and quarantine-reason breakdowns.
                </Typography>
              </Stack>
            </Panel>
          </Grid>
        </Grid>
      </Container>
    </Box>
  );
}

function MetricCard({ title, value, subtitle, icon, color }) {
  return (
    <Grid item xs={12} sm={6} md={4} xl={2}>
      <Paper
        elevation={0}
        sx={{
          p: 2,
          height: '100%',
          border: '1px solid',
          borderColor: 'divider',
          position: 'relative',
          overflow: 'hidden',
        }}
      >
        <Stack direction="row" justifyContent="space-between" alignItems="flex-start" gap={1.5}>
          <Box>
            <Typography color="text.secondary" sx={{ fontWeight: 700, fontSize: 14 }}>
              {title}
            </Typography>
            <Typography variant="h4" sx={{ mt: 0.7, overflowWrap: 'anywhere' }}>
              {value}
            </Typography>
            <Typography color="text.secondary" sx={{ mt: 0.6, fontSize: 13 }}>
              {subtitle}
            </Typography>
          </Box>
          <Box
            sx={{
              width: 42,
              height: 42,
              borderRadius: 2,
              display: 'grid',
              placeItems: 'center',
              color: '#ffffff',
              bgcolor: color,
              flex: '0 0 auto',
            }}
          >
            {icon}
          </Box>
        </Stack>
      </Paper>
    </Grid>
  );
}

function Panel({ title, action, children }) {
  return (
    <Paper
      elevation={0}
      sx={{
        p: { xs: 2, md: 2.5 },
        height: '100%',
        border: '1px solid',
        borderColor: 'divider',
      }}
    >
      <Stack direction="row" alignItems="center" justifyContent="space-between" gap={2} sx={{ mb: 1.5 }}>
        <Typography variant="h5">{title}</Typography>
        <Chip size="small" label={action} variant="outlined" />
      </Stack>
      {children}
    </Paper>
  );
}

function ReasonRow({ item, max }) {
  const percentOfMax = Math.max(3, (item.value / Math.max(max, 1)) * 100);
  return (
    <Box>
      <Stack direction="row" justifyContent="space-between" gap={1}>
        <Typography sx={{ fontSize: 14, overflowWrap: 'anywhere' }}>{item.name}</Typography>
        <Typography sx={{ fontSize: 14, fontWeight: 800 }}>{formatNumber(item.value)}</Typography>
      </Stack>
      <LinearProgress
        variant="determinate"
        value={percentOfMax}
        sx={{
          height: 9,
          borderRadius: 2,
          mt: 0.5,
          bgcolor: '#edf5f2',
          '& .MuiLinearProgress-bar': { bgcolor: '#bd3f53', borderRadius: 2 },
        }}
      />
    </Box>
  );
}

function ProfileLine({ label, value }) {
  return (
    <Stack direction="row" justifyContent="space-between" alignItems="center" gap={2}>
      <Typography color="text.secondary">{label}</Typography>
      <Chip label={value} size="small" color="primary" variant="outlined" />
    </Stack>
  );
}

function CenterStage({ children }) {
  return (
    <Box sx={{ minHeight: '100vh', display: 'grid', placeItems: 'center', p: 3 }}>{children}</Box>
  );
}

function normalizeSummary(summary) {
  const totals = summary.totals ?? {};
  const latency = summary.latency_ms ?? {};
  const producer = summary.producer ?? {};
  const eventCounts = summary.event_counts ?? {};
  const reasons = summary.quarantine_reasons ?? {};

  const eventData = objectToRows(eventCounts);
  const reasonData = objectToRows(reasons).sort((a, b) => b.value - a.value);
  const maxReason = reasonData.reduce((max, item) => Math.max(max, item.value), 0);

  return {
    totals: {
      valid: numberOrZero(totals.valid),
      quarantine: numberOrZero(totals.quarantine),
      alerts: numberOrZero(totals.alerts),
      processed: numberOrZero(totals.processed),
      acceptedPercent: numberOrZero(totals.accepted_percent),
    },
    latency: {
      avg: numberOrZero(latency.avg_ms),
      p50: numberOrZero(latency.p50_ms),
      p95: numberOrZero(latency.p95_ms),
      max: numberOrZero(latency.max_ms),
    },
    producer: {
      sent: numberOrZero(producer.sent),
      throughput: numberOrZero(producer.throughput_events_per_sec),
      patients: numberOrZero(producer.patients),
      allowedLateness: numberOrZero(producer.allowed_lateness_sec),
      invalidRate: numberOrZero(producer.invalid_rate),
      duplicateRate: numberOrZero(producer.duplicate_rate),
      outOfOrderRate: numberOrZero(producer.out_of_order_rate),
      lateRate: numberOrZero(producer.late_rate),
      seed: numberOrZero(producer.seed),
    },
    eventData,
    reasonData,
    maxReason,
    validityData: [
      { name: 'valid', value: numberOrZero(totals.valid) },
      { name: 'quarantined', value: numberOrZero(totals.quarantine) },
    ],
    performanceData: [
      { name: 'events/sec', value: numberOrZero(producer.throughput_events_per_sec) },
      { name: 'avg ms', value: numberOrZero(latency.avg_ms) },
      { name: 'p50 ms', value: numberOrZero(latency.p50_ms) },
      { name: 'p95 ms', value: numberOrZero(latency.p95_ms) },
    ],
  };
}

function objectToRows(value) {
  return Object.entries(value).map(([name, count]) => ({
    name,
    value: numberOrZero(count),
  }));
}

function numberOrZero(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function formatNumber(value) {
  return new Intl.NumberFormat('en-US').format(value);
}

function formatCompact(value) {
  return new Intl.NumberFormat('en-US', { notation: 'compact' }).format(value);
}

function percent(value) {
  return (numberOrZero(value) * 100).toFixed(1);
}

export default App;
