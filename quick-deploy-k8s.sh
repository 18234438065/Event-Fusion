#!/bin/bash

# 快速K8s部署脚本
# Quick Kubernetes Deployment Script

set -e

# 配置
NAMESPACE="event-fusion"
IMAGE_TAG="latest"
REGISTRY=""

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_banner() {
    echo -e "${BLUE}"
    echo "=============================================="
    echo "    事件融合服务 K8s 快速部署脚本"
    echo "=============================================="
    echo -e "${NC}"
}

print_step() {
    echo -e "${GREEN}[STEP]${NC} $1"
}

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_prerequisites() {
    print_step "检查前置条件..."
    
    # 检查kubectl
    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl 未安装"
        exit 1
    fi
    
    # 检查docker
    if ! command -v docker &> /dev/null; then
        print_error "docker 未安装"
        exit 1
    fi
    
    # 检查K8s连接
    if ! kubectl cluster-info &> /dev/null; then
        print_error "无法连接到K8s集群"
        exit 1
    fi
    
    print_info "✅ 前置条件检查通过"
}

build_images() {
    print_step "构建Docker镜像..."
    
    # 构建主服务镜像
    print_info "构建事件融合服务镜像..."
    docker build -f Dockerfile.ubuntu -t event-fusion:${IMAGE_TAG} .
    
    # 构建模拟接收服务镜像
    print_info "构建模拟接收服务镜像..."
    cd mock_services/event_receiver
    docker build -t mock-receiver:${IMAGE_TAG} .
    cd ../..
    
    # 构建视频处理服务镜像
    print_info "构建视频处理服务镜像..."
    cd services/video_processor
    docker build -t video-processor:${IMAGE_TAG} .
    cd ../..
    
    print_info "✅ Docker镜像构建完成"
}

import_images_containerd() {
    print_step "导入镜像到containerd..."
    
    # 导出镜像
    print_info "导出Docker镜像..."
    docker save event-fusion:${IMAGE_TAG} -o event-fusion.tar
    docker save mock-receiver:${IMAGE_TAG} -o mock-receiver.tar
    docker save video-processor:${IMAGE_TAG} -o video-processor.tar
    
    # 导入到containerd
    print_info "导入镜像到containerd..."
    sudo ctr -n=k8s.io images import event-fusion.tar
    sudo ctr -n=k8s.io images import mock-receiver.tar
    sudo ctr -n=k8s.io images import video-processor.tar
    
    # 清理临时文件
    rm -f event-fusion.tar mock-receiver.tar video-processor.tar
    
    print_info "✅ 镜像导入完成"
}

deploy_infrastructure() {
    print_step "部署基础设施..."
    
    # 创建命名空间
    kubectl create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -
    
    # 部署基础设施
    print_info "部署Redis..."
    kubectl apply -f deployment/k8s/redis/ -n ${NAMESPACE} || print_warning "Redis部署可能已存在"
    
    print_info "部署Zookeeper..."
    kubectl apply -f deployment/k8s/zookeeper/ -n ${NAMESPACE} || print_warning "Zookeeper部署可能已存在"
    
    print_info "部署Kafka..."
    kubectl apply -f deployment/k8s/kafka/ -n ${NAMESPACE} || print_warning "Kafka部署可能已存在"
    
    print_info "部署Elasticsearch..."
    kubectl apply -f deployment/k8s/elasticsearch/ -n ${NAMESPACE} || print_warning "Elasticsearch部署可能已存在"
    
    print_info "✅ 基础设施部署完成"
}

wait_for_infrastructure() {
    print_step "等待基础设施就绪..."
    
    print_info "等待Redis就绪..."
    kubectl wait --for=condition=ready pod -l app=redis -n ${NAMESPACE} --timeout=300s
    
    print_info "等待Zookeeper就绪..."
    kubectl wait --for=condition=ready pod -l app=zookeeper -n ${NAMESPACE} --timeout=300s
    
    print_info "等待Kafka就绪..."
    kubectl wait --for=condition=ready pod -l app=kafka -n ${NAMESPACE} --timeout=300s
    
    print_info "等待Elasticsearch就绪..."
    kubectl wait --for=condition=ready pod -l app=elasticsearch -n ${NAMESPACE} --timeout=300s
    
    print_info "✅ 基础设施就绪"
}

deploy_services() {
    print_step "部署应用服务..."
    
    # 部署完整服务
    print_info "部署所有服务..."
    kubectl apply -f deployment/k8s/complete/all-services.yaml
    
    print_info "✅ 应用服务部署完成"
}

wait_for_services() {
    print_step "等待服务就绪..."
    
    print_info "等待事件融合服务就绪..."
    kubectl wait --for=condition=ready pod -l app=event-fusion-service -n ${NAMESPACE} --timeout=300s
    
    print_info "等待模拟接收服务就绪..."
    kubectl wait --for=condition=ready pod -l app=mock-receiver -n ${NAMESPACE} --timeout=300s
    
    print_info "等待视频处理服务就绪..."
    kubectl wait --for=condition=ready pod -l app=video-processor -n ${NAMESPACE} --timeout=300s
    
    print_info "✅ 所有服务就绪"
}

verify_deployment() {
    print_step "验证部署..."
    
    # 获取节点IP
    NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
    
    print_info "检查服务状态..."
    kubectl get pods -n ${NAMESPACE}
    
    print_info "检查服务访问..."
    kubectl get services -n ${NAMESPACE}
    
    print_info "测试健康检查..."
    if curl -s http://${NODE_IP}:30800/health > /dev/null; then
        print_info "✅ 事件融合服务健康检查通过"
    else
        print_warning "⚠️ 事件融合服务健康检查失败"
    fi
    
    if curl -s http://${NODE_IP}:30801/health > /dev/null; then
        print_info "✅ 模拟接收服务健康检查通过"
    else
        print_warning "⚠️ 模拟接收服务健康检查失败"
    fi
    
    print_info "✅ 部署验证完成"
    
    echo ""
    echo "🎉 部署完成！"
    echo "📱 服务访问地址:"
    echo "   事件融合服务: http://${NODE_IP}:30800"
    echo "   模拟接收服务: http://${NODE_IP}:30801"
    echo "   Kibana: http://${NODE_IP}:30601"
    echo ""
    echo "🧪 运行演示:"
    echo "   python demo_complete_project.py --k8s"
    echo ""
}

# 主函数
main() {
    print_banner
    
    # 解析参数
    SKIP_BUILD=false
    SKIP_IMPORT=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-build)
                SKIP_BUILD=true
                shift
                ;;
            --skip-import)
                SKIP_IMPORT=true
                shift
                ;;
            --image-tag)
                IMAGE_TAG="$2"
                shift 2
                ;;
            --namespace)
                NAMESPACE="$2"
                shift 2
                ;;
            -h|--help)
                echo "用法: $0 [选项]"
                echo "选项:"
                echo "  --skip-build     跳过镜像构建"
                echo "  --skip-import    跳过镜像导入"
                echo "  --image-tag TAG  镜像标签 (默认: latest)"
                echo "  --namespace NS   命名空间 (默认: event-fusion)"
                echo "  -h, --help       显示此帮助"
                exit 0
                ;;
            *)
                print_error "未知参数: $1"
                exit 1
                ;;
        esac
    done
    
    print_info "配置参数:"
    print_info "  命名空间: ${NAMESPACE}"
    print_info "  镜像标签: ${IMAGE_TAG}"
    print_info "  跳过构建: ${SKIP_BUILD}"
    print_info "  跳过导入: ${SKIP_IMPORT}"
    echo ""
    
    # 执行部署步骤
    check_prerequisites
    
    if [ "$SKIP_BUILD" = false ]; then
        build_images
    fi
    
    if [ "$SKIP_IMPORT" = false ]; then
        import_images_containerd
    fi
    
    deploy_infrastructure
    wait_for_infrastructure
    deploy_services
    wait_for_services
    verify_deployment
}

# 错误处理
trap 'print_error "部署过程中出现错误，退出码: $?"' ERR

# 运行主函数
main "$@"
