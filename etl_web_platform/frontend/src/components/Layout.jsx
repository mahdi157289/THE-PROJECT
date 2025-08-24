import { useState } from 'react'
import { Link, useLocation } from 'react-router-dom'
import { 
  HiHome, 
  HiDatabase, 
  HiCog, 
  HiChartBar, 
  HiChartPie,
  HiMenu,
  HiX,
  HiChevronLeft,
  HiChevronRight
} from 'react-icons/hi'

// Import the actual logo image
import logoImage from '../assets/image.jpg'

// Bourse de Tunis Logo Component
const BourseLogo = ({ size = "w-12 h-12", showText = true }) => {
  return (
    <div className="flex items-center">
      {/* Logo Icon - Using the actual image */}
      <img 
        src={logoImage} 
        alt="Bourse de Tunis Logo" 
        className={`${size} object-contain`}
      />
      
      {/* Logo Text */}
      {showText && (
        <div className="ml-3 text-left">
          <div className="text-sm font-bold text-gray-900 leading-tight">بورصة تونس</div>
          <div className="w-full h-px bg-gray-900 my-1"></div>
          <div className="text-sm font-bold text-gray-900 leading-tight">BOURSE DE TUNIS</div>
        </div>
      )}
    </div>
  )
}

const navigation = [
  { name: 'Dashboard', href: '/', icon: HiHome },
  { name: 'ETL Pipeline', href: '/etl-pipeline', icon: HiDatabase },
  { name: 'Scraping Monitor', href: '/scraping-monitor', icon: HiCog },
  { name: 'Power BI Dashboards', href: '/powerbi-dashboards', icon: HiChartBar },
  { name: 'Analytics', href: '/analytics', icon: HiChartPie },
]

export default function Layout({ children }) {
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const location = useLocation()

  // Toggle function for sidebar collapse
  const toggleSidebar = () => {
    setSidebarCollapsed(!sidebarCollapsed)
  }

  // Close mobile sidebar
  const closeMobileSidebar = () => {
    setSidebarOpen(false)
  }

  // Open mobile sidebar
  const openMobileSidebar = () => {
    setSidebarOpen(true)
  }

  return (
    <div className="flex h-screen bg-gray-100">
      {/* Mobile sidebar */}
      <div className={`fixed inset-0 z-50 lg:hidden ${sidebarOpen ? 'block' : 'hidden'}`}>
        <div className="fixed inset-0 bg-gray-600 bg-opacity-75" onClick={closeMobileSidebar} />
        <div className="fixed inset-y-0 left-0 flex w-64 flex-col bg-white">
          <div className="flex h-16 items-center justify-between px-4">
            <BourseLogo size="w-8 h-8" showText={true} />
            <button
              onClick={closeMobileSidebar}
              className="text-gray-400 hover:text-gray-600 p-2 rounded-md hover:bg-gray-100"
            >
              <HiX className="h-6 w-6" />
            </button>
          </div>
          <nav className="flex-1 space-y-1 px-2 py-4">
            {navigation.map((item) => {
              const isActive = location.pathname === item.href
              return (
                <Link
                  key={item.name}
                  to={item.href}
                  onClick={closeMobileSidebar}
                  className={`sidebar-item ${isActive ? 'active' : ''}`}
                >
                  <item.icon className="h-5 w-5" />
                  <span className="ml-3">{item.name}</span>
                </Link>
              )
            })}
          </nav>
        </div>
      </div>

      {/* Desktop sidebar */}
      <div className={`hidden lg:flex lg:flex-shrink-0 transition-all duration-300 ease-in-out ${sidebarCollapsed ? 'w-16' : 'w-64'}`}>
        <div className="flex w-full flex-col">
          <div className="flex h-16 items-center justify-between px-4 bg-white border-b border-gray-200">
            {!sidebarCollapsed ? (
              <BourseLogo size="w-8 h-8" showText={true} />
            ) : (
              <BourseLogo size="w-8 h-8" showText={false} />
            )}
            <button
              onClick={toggleSidebar}
              className="p-2 rounded-md hover:bg-gray-100 transition-colors duration-200 flex-shrink-0"
              title={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
            >
              {sidebarCollapsed ? (
                <HiChevronRight className="h-5 w-5 text-gray-600" />
              ) : (
                <HiChevronLeft className="h-5 w-5 text-gray-600" />
              )}
            </button>
          </div>
          <nav className="flex-1 space-y-1 px-2 py-4 bg-white">
            {navigation.map((item) => {
              const isActive = location.pathname === item.href
              return (
                <Link
                  key={item.name}
                  to={item.href}
                  className={`sidebar-item ${isActive ? 'active' : ''} ${sidebarCollapsed ? 'justify-center px-2' : ''}`}
                  title={sidebarCollapsed ? item.name : ''}
                >
                  <item.icon className="h-5 w-5 flex-shrink-0" />
                  {!sidebarCollapsed && <span className="ml-3">{item.name}</span>}
                </Link>
              )
            })}
          </nav>
        </div>
      </div>

      {/* Main content */}
      <div className="flex flex-1 flex-col overflow-hidden">
        {/* Top bar */}
        <div className="flex h-16 items-center justify-between px-4 bg-white border-b border-gray-200 lg:hidden">
          <button
            onClick={openMobileSidebar}
            className="text-gray-400 hover:text-gray-600 p-2 rounded-md hover:bg-gray-100"
          >
            <HiMenu className="h-6 w-6" />
          </button>
          <BourseLogo size="w-8 h-8" showText={true} />
          <div className="w-6" />
        </div>

        {/* Page content */}
        <main className="flex-1 overflow-y-auto p-4">
          <div className="transition-all duration-300">
            {children}
          </div>
        </main>
      </div>
    </div>
  )
}
