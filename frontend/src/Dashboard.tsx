import { useState, useEffect } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
)

interface Lab {
  id: number
  name: string
}

interface ScoreBucket {
  bucket: string
  count: number
}

interface ScoresResponse {
  lab_id: number
  buckets: ScoreBucket[]
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface TimelineResponse {
  lab_id: number
  timeline: TimelineEntry[]
}

interface TaskPassRate {
  task_id: number
  task_name: string
  pass_rate: number
}

interface PassRatesResponse {
  lab_id: number
  tasks: TaskPassRate[]
}

interface DashboardData {
  scores: ScoresResponse | null
  timeline: TimelineResponse | null
  passRates: PassRatesResponse | null
}

const API_BASE = ''

function getAuthHeaders(): HeadersInit {
  const apiKey = localStorage.getItem('api_key')
  return {
    Authorization: `Bearer ${apiKey}`,
    'Content-Type': 'application/json',
  }
}

async function fetchWithAuth<T>(url: string): Promise<T> {
  const res = await fetch(url, { headers: getAuthHeaders() })
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`)
  }
  return res.json() as Promise<T>
}

export default function Dashboard() {
  const [labs, setLabs] = useState<Lab[]>([])
  const [selectedLabId, setSelectedLabId] = useState<number | null>(null)
  const [data, setData] = useState<DashboardData>({
    scores: null,
    timeline: null,
    passRates: null,
  })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetchWithAuth<Lab[]>('/analytics/labs')
      .then(setLabs)
      .catch((err: Error) => setError(err.message))
  }, [])

  useEffect(() => {
    if (!selectedLabId) return

    setLoading(true)
    setError(null)

    const labParam = `lab=${selectedLabId}`

    Promise.all([
      fetchWithAuth<ScoresResponse>(`/analytics/scores?${labParam}`),
      fetchWithAuth<TimelineResponse>(`/analytics/timeline?${labParam}`),
      fetchWithAuth<PassRatesResponse>(`/analytics/pass-rates?${labParam}`),
    ])
      .then(([scores, timeline, passRates]) => {
        setData({ scores, timeline, passRates })
        setLoading(false)
      })
      .catch((err: Error) => {
        setError(err.message)
        setLoading(false)
      })
  }, [selectedLabId])

  const scoreChartData = data.scores
    ? {
        labels: data.scores.buckets.map((b) => b.bucket),
        datasets: [
          {
            label: 'Number of Students',
            data: data.scores.buckets.map((b) => b.count),
            backgroundColor: 'rgba(54, 162, 235, 0.6)',
            borderColor: 'rgba(54, 162, 235, 1)',
            borderWidth: 1,
          },
        ],
      }
    : null

  const timelineChartData = data.timeline
    ? {
        labels: data.timeline.timeline.map((t) => t.date),
        datasets: [
          {
            label: 'Submissions',
            data: data.timeline.timeline.map((t) => t.submissions),
            borderColor: 'rgba(75, 192, 192, 1)',
            backgroundColor: 'rgba(75, 192, 192, 0.2)',
            tension: 0.1,
          },
        ],
      }
    : null

  const barOptions = {
    responsive: true,
    plugins: {
      legend: {
        display: true,
      },
      title: {
        display: true,
        text: 'Score Distribution',
      },
    },
  }

  const lineOptions = {
    responsive: true,
    plugins: {
      legend: {
        display: true,
      },
      title: {
        display: true,
        text: 'Submissions Over Time',
      },
    },
  }

  return (
    <div className="dashboard">
      <h1>Dashboard</h1>

      <div className="lab-selector">
        <label htmlFor="lab-select">Select Lab: </label>
        <select
          id="lab-select"
          value={selectedLabId ?? ''}
          onChange={(e) =>
            setSelectedLabId(
              e.target.value ? Number(e.target.value) : null,
            )
          }
        >
          <option value="">-- Choose a lab --</option>
          {labs.map((lab) => (
            <option key={lab.id} value={lab.id}>
              {lab.name}
            </option>
          ))}
        </select>
      </div>

      {loading && <p>Loading dashboard data...</p>}
      {error && <p className="error">Error: {error}</p>}

      {!loading && !error && selectedLabId && (
        <div className="dashboard-content">
          <div className="chart-container">
            {scoreChartData && <Bar options={barOptions} data={scoreChartData} />}
          </div>

          <div className="chart-container">
            {timelineChartData && (
              <Line options={lineOptions} data={timelineChartData} />
            )}
          </div>

          <div className="table-container">
            <h2>Pass Rates per Task</h2>
            {data.passRates && data.passRates.tasks.length > 0 ? (
              <table>
                <thead>
                  <tr>
                    <th>Task</th>
                    <th>Pass Rate</th>
                  </tr>
                </thead>
                <tbody>
                  {data.passRates.tasks.map((task) => (
                    <tr key={task.task_id}>
                      <td>{task.task_name}</td>
                      <td>{(task.pass_rate * 100).toFixed(1)}%</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <p>No pass rate data available</p>
            )}
          </div>
        </div>
      )}

      {!selectedLabId && !loading && (
        <p>Select a lab to view analytics</p>
      )}
    </div>
  )
}
